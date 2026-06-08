package webrtc

import (
	"context"
	"encoding/json"

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

func roomChannel(roomID string) string {
	return "room:" + roomID + ":frames"
}

func (b *RedisBroadcaster) Publish(ctx context.Context, roomID string, notif FrameNotification) {
	data, err := json.Marshal(notif)
	if err != nil {
		b.logger.Error("broadcaster marshal failed", zap.Error(err))
		return
	}
	if err := b.client.Publish(ctx, roomChannel(roomID), data).Err(); err != nil {
		b.logger.Warn("broadcaster publish failed", zap.String("room_id", roomID), zap.Error(err))
	}	
}

func (b *RedisBroadcaster) Subscribe(ctx context.Context, roomID string) <-chan FrameNotification {
	out := make(chan FrameNotification, 32)
	go func() {
		defer close(out)
		pubsub := b.client.Subscribe(ctx, roomChannel(roomID))
		defer pubsub.Close()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // admin disconnect
				}
				b.logger.Warn("broadcaster receive failed", 
					zap.String("room_id", roomID), 
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


func roomEventsChannel(roomID string) string {
	return "room:" + roomID + ":events"
}

func (b *RedisBroadcaster) PublishParticipantEvent(ctx context.Context, roomID string, event domain.ParticipantEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		b.logger.Error("participant event marshal failed", zap.Error(err))
		return
	}
	if err := b.client.Publish(ctx, roomEventsChannel(roomID), data).Err(); err != nil {
		b.logger.Warn("participant event published failed", 
			zap.String("room_id", roomID), 
			zap.Error(err),
		)
	}
}

func (b *RedisBroadcaster) SubscribeEvents(ctx context.Context, roomID string) <-chan domain.ParticipantEvent {
	out := make(chan domain.ParticipantEvent, 16)
	go func() {
		defer close(out)
		pubsub := b.client.Subscribe(ctx, roomEventsChannel(roomID))
		defer pubsub.Close()

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.logger.Warn("participant event receive failed", zap.String("room_id", roomID), zap.Error(err))
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

