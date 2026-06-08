package webrtc

import "sync"

type sessionKey struct {
	roomID        string
	participantID string
	streamType    string
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[sessionKey]*Peer
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[sessionKey]*Peer),
	}
}

func (m *SessionManager) Add(roomID, participantID, streamType string, p *Peer) {
	key := sessionKey{
		roomID:        roomID,
		participantID: participantID,
		streamType:    streamType,
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.sessions[key]; ok {
		old.close()
	}
	m.sessions[key] = p
}

func (m *SessionManager) Remove(roomID, participantID, streamType string) {
	m.mu.Lock()
	delete(m.sessions, sessionKey{
		roomID:        roomID,
		participantID: participantID,
		streamType:    streamType,
	})
	m.mu.Unlock()
}

func (m *SessionManager) RoomPeers(roomID string) []*Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Peer
	for k, p := range m.sessions {
		if k.roomID == roomID {
			out = append(out, p)
		}
	}
	return out
}

func (m *SessionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}
