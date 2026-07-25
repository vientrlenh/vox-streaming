package webrtc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

type FrameNotification struct {
	StreamID   string `json:"streamId"`
	StreamType string `json:"streamType"`
	FrameURL   string `json:"frameUrl"`
	SequenceNo int64  `json:"sequenceNo"`
}

type RedisBroadcaster struct {
	client *redis.Client
	logger *zap.Logger
}


func NewRedisBroadcaster(client *redis.Client, logger *zap.Logger) *RedisBroadcaster {
	return &RedisBroadcaster{
		client: client, 
		logger: logger,
	}
}

func scheduleChannel(scheduleID string) string {
	return "schedule:" + scheduleID + ":frames"
}

func (b *RedisBroadcaster) Publish(ctx context.Context, scheduleID string, notif FrameNotification) {
	data, err := json.Marshal(notif)
	if err != nil {
		b.logger.Error("broadcaster marshal failed", zap.Error(err))
		return
	}
	if err := b.client.Publish(ctx, scheduleChannel(scheduleID), data).Err(); err != nil {
		b.logger.Warn("broadcaster publish failed", zap.String("schedule_id", scheduleID), zap.Error(err))
	}	
}

func (b *RedisBroadcaster) Subscribe(ctx context.Context, scheduleID string) <-chan FrameNotification {
	out := make(chan FrameNotification, 32)
	go func() {
		defer close(out)
		pubsub := b.client.Subscribe(ctx, scheduleChannel(scheduleID))
		defer pubsub.Close()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // admin disconnect
				}
				b.logger.Warn("broadcaster receive failed", 
					zap.String("scheduleId", scheduleID), 
					zap.Error(err),
				)
				return
			}
			var notif FrameNotification
			if err := json.Unmarshal([]byte(msg.Payload), &notif); err != nil {
				b.logger.Warn("broadcaster unmarshal failed", zap.Error(err))
				continue
			}

			select {
			case out<-notif:
			case<-ctx.Done():
				return
			default:
				// admin client lag, bỏ frame này, 5s sau nhận frame mới
			}
		}
	}()
	return out
}


func scheduleEventsChannel(scheduleID string) string {
	return "schedule:" + scheduleID + ":events"
}

func (b *RedisBroadcaster) PublishParticipantEvent(ctx context.Context, scheduleID string, event domain.ParticipantEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		b.logger.Error("participant event marshal failed", zap.Error(err))
		return
	}
	if err := b.client.Publish(ctx, scheduleEventsChannel(scheduleID), data).Err(); err != nil {
		b.logger.Warn("participant event published failed", 
			zap.String("scheduleId", scheduleID), 
			zap.Error(err),
		)
	}
}

func (b *RedisBroadcaster) SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent {
	out := make(chan domain.ParticipantEvent, 16)
	go func() {
		defer close(out)
		pubsub := b.client.Subscribe(ctx, scheduleEventsChannel(scheduleID))
		defer pubsub.Close()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.logger.Warn("participant event receive failed", zap.String("schedule_id", scheduleID), zap.Error(err))
				return
			}

			var event domain.ParticipantEvent
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				continue
			}
			select {
			case out<-event:
			case<-ctx.Done():
				return
			default:
			}
		}
	}()
	return out
}

func scheduleAlertsChannel(scheduleID string) string {
	return "schedule:" + scheduleID + ":alerts"
}

func (b *RedisBroadcaster) PublishAlertEvent(ctx context.Context, scheduleID string, alert domain.AlertEvent) error {
	data, err := json.Marshal(alert)
	if err != nil {
		b.logger.Error("alert marshal failed", zap.Error(err))
		return fmt.Errorf("marshal alert: %w", err)
	}
	if err := b.client.Publish(ctx, scheduleAlertsChannel(scheduleID), data).Err(); err != nil {
		b.logger.Warn("alert publish failed", 
			zap.String("scheduleId", scheduleID), 
			zap.Error(err),
		)
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

func (b *RedisBroadcaster) SubscribeAlerts(ctx context.Context, scheduleID string) <-chan domain.AlertEvent {
	out := make(chan domain.AlertEvent, 16)
	go func() {
		defer close(out)
		pubsub := b.client.Subscribe(ctx, scheduleAlertsChannel(scheduleID))
		defer pubsub.Close()
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			var alert domain.AlertEvent
			if err := json.Unmarshal([]byte(msg.Payload), &alert); err != nil {
				continue
			}
			select {
			case out<-alert:
			case<-ctx.Done():
				return
			default:
			}
		}
	}()
	return out
}

// is the schedule has teacher subscribe (pubsub numsub)
func (b *RedisBroadcaster) HasMonitor(ctx context.Context, scheduleID string) (bool, error) {
	res, err := b.client.PubSubNumSub(ctx, scheduleChannel(scheduleID)).Result()
	if err != nil {
		return false, nil
	}
	return res[scheduleChannel(scheduleID)] > 0, nil
}


func (b *RedisBroadcaster) PublishFrameURL(ctx context.Context, scheduleID, streamID, streamType, frameURL string, seq int64) {
	b.Publish(ctx, scheduleID, FrameNotification{
		StreamID: streamID, 
		StreamType: streamType,
		FrameURL: frameURL,
		SequenceNo: seq,
	})
}

