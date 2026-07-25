package webrtc

import "testing"

// Add/Replace/RemoveIfSame never need to invoke Peer.close() when there is no
// eviction, so a bare &Peer{} is a safe opaque token here: these tests only
// exercise pointer identity and map bookkeeping, not real WebRTC/recording
// teardown (which needs a fully wired Peer and is out of scope for a unit test).

func TestSessionManager_Add_NewKey(t *testing.T) {
	m := NewSessionManager()
	p := &Peer{}

	m.Add("schedule-1", "participant-1", "camera", p)

	peers := m.SchedulePeers("schedule-1")
	if len(peers) != 1 || peers[0] != p {
		t.Fatalf("got %v, want exactly [p]", peers)
	}
	if m.Count() != 1 {
		t.Fatalf("got Count()=%d, want 1", m.Count())
	}
}

func TestSessionManager_Replace(t *testing.T) {
	m := NewSessionManager()
	p1 := &Peer{}
	p2 := &Peer{}

	if old := m.Replace("schedule-1", "participant-1", "camera", p1); old != nil {
		t.Fatalf("got old=%v, want nil on first Replace for a fresh key", old)
	}
	if old := m.Replace("schedule-1", "participant-1", "camera", p2); old != p1 {
		t.Fatalf("got old=%v, want p1 (the peer being replaced)", old)
	}
	if m.Count() != 1 {
		t.Fatalf("got Count()=%d, want 1 (replace overwrites, not appends)", m.Count())
	}

	peers := m.SchedulePeers("schedule-1")
	if len(peers) != 1 || peers[0] != p2 {
		t.Fatalf("got %v, want exactly [p2]", peers)
	}
}

func TestSessionManager_RemoveIfSame(t *testing.T) {
	m := NewSessionManager()
	stale := &Peer{}
	current := &Peer{}

	m.Replace("schedule-1", "participant-1", "camera", stale)
	m.Replace("schedule-1", "participant-1", "camera", current) // simulates a reconnect race: stale's owner reconnects and replaces it before stale's deferred cleanup runs

	// The stale peer's own deferred cleanup fires RemoveIfSame with itself,
	// not with the peer that replaced it — this must be a no-op so the new
	// connection is not evicted by the old one's teardown.
	m.RemoveIfSame("schedule-1", "participant-1", "camera", stale)
	if peers := m.SchedulePeers("schedule-1"); len(peers) != 1 || peers[0] != current {
		t.Fatalf("got %v, want [current] to survive RemoveIfSame(stale)", peers)
	}

	m.RemoveIfSame("schedule-1", "participant-1", "camera", current)
	if peers := m.SchedulePeers("schedule-1"); len(peers) != 0 {
		t.Fatalf("got %v, want no peers after RemoveIfSame(current)", peers)
	}
}

func TestSessionManager_Remove(t *testing.T) {
	m := NewSessionManager()
	m.Replace("schedule-1", "participant-1", "camera", &Peer{})

	m.Remove("schedule-1", "participant-1", "camera")

	if m.Count() != 0 {
		t.Fatalf("got Count()=%d, want 0 after Remove", m.Count())
	}
}

func TestSessionManager_SchedulePeers_FiltersBySchedule(t *testing.T) {
	m := NewSessionManager()
	p1 := &Peer{}
	p2 := &Peer{}
	p3 := &Peer{}

	m.Replace("schedule-1", "participant-1", "camera", p1)
	m.Replace("schedule-1", "participant-2", "screen", p2)
	m.Replace("schedule-2", "participant-3", "camera", p3)

	peers := m.SchedulePeers("schedule-1")
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2 for schedule-1", len(peers))
	}
	for _, p := range peers {
		if p != p1 && p != p2 {
			t.Errorf("unexpected peer %v leaked from another schedule", p)
		}
	}

	if m.Count() != 3 {
		t.Fatalf("got Count()=%d, want 3 across all schedules", m.Count())
	}
}
