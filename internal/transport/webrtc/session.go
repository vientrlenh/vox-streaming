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
	old, ok := m.sessions[key]
	m.sessions[key] = p
	m.mu.Unlock()

	// Outside the lock, and in its own goroutine, for the same reason ServeStream detaches its
	// replacement close: Peer.close drains segment uploads under a 60s cap. Holding the write lock
	// across that would block EVERY session operation process-wide -- Replace, RemoveIfSame and
	// SchedulePeers all contend on this one mutex, so a single slow S3 drain would freeze new
	// connections and monitor snapshots for every schedule on the instance, not just this one.
	if ok && old != nil {
		go old.Close()
	}
}

// Get returns the peer currently registered for this key, or nil. Used by ServeStream to find a
// peer worth adopting when a student's signaling socket comes back; the caller still has to prove
// the reconnect refers to that exact stream and that the peer is alive.
func (m *SessionManager) Get(scheduleID, participantID, streamType string) *Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[sessionKey{
		scheduleID:    scheduleID,
		participantID: participantID,
		streamType:    streamType,
	}]
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
