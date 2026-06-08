package webrtc

import "sync"

type FrameNotification struct {
	StreamID   string `json:"streamId"`
	StreamType string `json:"streamType"`
	FrameURL   string `json:"frameUrl"`
	SequenceNo int64  `json:"sequenceNo"`
}

type RoomBroadcaster struct {
	mu sync.RWMutex
	subscribers map[string][]chan FrameNotification 
}


func NewRoomBroadcaster() *RoomBroadcaster {
	return &RoomBroadcaster{
		subscribers: make(map[string][]chan FrameNotification),
	}
}

func (b *RoomBroadcaster) Subscribe(roomID string) chan FrameNotification {
	ch := make(chan FrameNotification, 32)
	b.mu.Lock()
	b.subscribers[roomID] = append(b.subscribers[roomID], ch)
	b.mu.Unlock()
	return ch
}

func (b *RoomBroadcaster) Unsubscribe(roomID string, ch chan FrameNotification) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subscribers[roomID]
	for i, s := range subs {
		if s == ch {
			b.subscribers[roomID] = append(subs[:i], subs[i+1:]...)
			close(ch)
			return
		}
	}
}

func (b *RoomBroadcaster) Publish(roomID string, notif FrameNotification) {
	b.mu.RLock()
	subs := b.subscribers[roomID]
	b.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch<-notif:
		default:

		}
	}
}
