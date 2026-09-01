package webrtc

import (
	"testing"

	"go.uber.org/zap"
)

// These tests cover the signaling binding in isolation: generation bookkeeping and the
// closed-peer refusal, which is all that stands between a reconnect and a peer being torn down by
// the connection it just replaced. A bare Peer is enough — nothing here reaches the WebRTC or
// recording machinery, and the grace timer is never allowed to fire (signalingGrace is 90s).
func newSignalingTestPeer() *Peer {
	return &Peer{
		done:     make(chan struct{}),
		streamID: "stream-1",
		logger:   zap.NewNop(),
	}
}

func TestBindSignaling_RefusesClosedPeer(t *testing.T) {
	p := newSignalingTestPeer()
	close(p.done)

	if _, ok := p.BindSignaling(&safeConn{}); ok {
		t.Fatal("got ok=true binding to a closed peer, want refusal — adoption must never hand a caller a peer that is already tearing down")
	}
}

func TestBindSignaling_GenerationAdvancesPerBind(t *testing.T) {
	p := newSignalingTestPeer()

	first, ok := p.BindSignaling(&safeConn{})
	if !ok {
		t.Fatal("first bind refused on a live peer")
	}
	second, ok := p.BindSignaling(&safeConn{})
	if !ok {
		t.Fatal("second bind refused on a live peer")
	}
	if first == second {
		t.Fatalf("got the same generation %d twice, want distinct — ReleaseSignaling tells connections apart by this alone", first)
	}
}

// The invariant the whole design rests on: when a reconnect adopts a peer, the OLD connection's
// unwind must not arm a grace timer. If it did, the adopted-and-healthy peer would be closed
// signalingGrace later for a socket that had already been superseded.
func TestReleaseSignaling_StaleGenerationIsNoOp(t *testing.T) {
	p := newSignalingTestPeer()
	stale, _ := p.BindSignaling(&safeConn{})

	current := &safeConn{}
	currentGen, _ := p.BindSignaling(current) // the reconnect adopts the peer

	if p.ReleaseSignaling(stale) {
		t.Fatal("got true releasing a superseded generation, want false — the old connection must not take the peer down with it")
	}

	p.signalMu.Lock()
	conn, timer, gen := p.signalConn, p.signalTimer, p.signalGen
	p.signalMu.Unlock()

	if conn != current {
		t.Error("stale release detached the adopting connection's socket")
	}
	if timer != nil {
		t.Error("stale release armed a grace timer; the adopted peer would be closed out from under a live stream")
	}
	if gen != currentGen {
		t.Errorf("got generation %d, want %d unchanged by a stale release", gen, currentGen)
	}
}

func TestReleaseSignaling_CurrentGenerationArmsGrace(t *testing.T) {
	p := newSignalingTestPeer()
	gen, _ := p.BindSignaling(&safeConn{})

	if !p.ReleaseSignaling(gen) {
		t.Fatal("got false releasing the current generation, want true")
	}

	p.signalMu.Lock()
	conn, timer := p.signalConn, p.signalTimer
	p.signalMu.Unlock()

	if conn != nil {
		t.Error("released connection is still bound")
	}
	if timer == nil {
		t.Fatal("no grace timer armed; a peer whose client never returns would leak for the rest of the exam")
	}
	if !timer.Stop() {
		t.Error("grace timer had already fired or been stopped")
	}
}

// A rebind after a release must disarm the grace, otherwise the timer armed by the dropped socket
// still fires signalingGrace later and closes a stream that reconnected seconds after dropping.
func TestBindSignaling_AfterReleaseDisarmsGrace(t *testing.T) {
	p := newSignalingTestPeer()
	gen, _ := p.BindSignaling(&safeConn{})
	p.ReleaseSignaling(gen)

	p.signalMu.Lock()
	armed := p.signalTimer
	p.signalMu.Unlock()
	if armed == nil {
		t.Fatal("precondition failed: release did not arm a grace timer")
	}

	if _, ok := p.BindSignaling(&safeConn{}); !ok {
		t.Fatal("rebind refused on a peer still inside its grace window")
	}

	p.signalMu.Lock()
	timer := p.signalTimer
	p.signalMu.Unlock()
	if timer != nil {
		t.Fatal("grace timer survived the rebind; the reconnected stream would be closed anyway")
	}
	if armed.Stop() {
		t.Error("the previously armed timer was never stopped by the rebind")
	}
}

// The deliberate-stop path: runSignaling handles "bye" by closing the peer inline, and the caller's
// deferred release then arrives to find it gone. That release must arm nothing -- a grace timer on a
// dead peer pins it for 90s and ends by setting closedByFailure, the exact flag the bye path exists
// to keep off a clean exam end.
func TestReleaseSignaling_ClosedPeerArmsNothing(t *testing.T) {
	p := newSignalingTestPeer()
	gen, _ := p.BindSignaling(&safeConn{})

	close(p.done) // stands in for peer.Close() on the bye path

	if p.ReleaseSignaling(gen) {
		t.Fatal("got true releasing a closed peer, want false")
	}

	p.signalMu.Lock()
	timer := p.signalTimer
	p.signalMu.Unlock()
	if timer != nil {
		t.Fatal("armed a grace timer on a peer that is already closed")
	}
}

// A peer between connections drops signaling writes rather than panicking. Everything sent this
// way is per-negotiation, so there is nothing worth queueing for the next socket.
func TestWriteSignal_NoBoundConnection(t *testing.T) {
	p := newSignalingTestPeer()

	if err := p.writeSignal(SignalMessage{Type: "ice-candidate"}); err != nil {
		t.Fatalf("got err=%v writing with no bound connection, want nil", err)
	}
}

func TestIsAlive_TracksDone(t *testing.T) {
	p := newSignalingTestPeer()
	if !p.IsAlive() {
		t.Fatal("got IsAlive()=false on a fresh peer")
	}

	close(p.done)
	if p.IsAlive() {
		t.Fatal("got IsAlive()=true after done closed")
	}
}
