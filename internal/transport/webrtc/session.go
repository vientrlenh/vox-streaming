package webrtc

import "sync"

type sessionKey struct {
	scheduleID        string
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

func (m *SessionManager) Add(scheduleID, participantID, streamType string, p *Peer) {
	key := sessionKey{
		scheduleID:        scheduleID,
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

func (m *SessionManager) Replace(scheduleID, participantID, streamType string, p *Peer) *Peer {
	key := sessionKey{
		scheduleID: scheduleID, 
		participantID: participantID,
		streamType: streamType,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.sessions[key] // nil if it is not created
	m.sessions[key] = p
	return old
}

func (m *SessionManager) RemoveIfSame(scheduleID, participantID, streamType string, p *Peer) {
	key := sessionKey{
		scheduleID: scheduleID, 
		participantID: participantID,
		streamType: streamType,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.sessions[key]; ok && current == p {
		delete(m.sessions, key)
	}
}

func (m *SessionManager) Remove(scheduleID, participantID, streamType string) {
	m.mu.Lock()
	delete(m.sessions, sessionKey{
		scheduleID:        scheduleID,
		participantID: participantID,
		streamType:    streamType,
	})
	m.mu.Unlock()
}

func (m *SessionManager) SchedulePeers(scheduleID string) []*Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Peer
	for k, p := range m.sessions {
		if k.scheduleID == scheduleID {
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
