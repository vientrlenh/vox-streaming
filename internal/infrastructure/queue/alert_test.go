package queue

import (
	"testing"
	"time"
)

func TestChainAlert(t *testing.T) {
	t.Run("invokes every function in order with the same alert", func(t *testing.T) {
		var calls []string
		var received []Alert

		fn1 := func(a Alert) { calls = append(calls, "fn1"); received = append(received, a) }
		fn2 := func(a Alert) { calls = append(calls, "fn2"); received = append(received, a) }
		fn3 := func(a Alert) { calls = append(calls, "fn3"); received = append(received, a) }

		alert := Alert{Topic: "exam.frame.ready", GroupID: "g1", Level: AlertCritical, Value: 10, Threshold: 5, At: time.Now()}

		ChainAlert(fn1, fn2, fn3)(alert)

		if len(calls) != 3 || calls[0] != "fn1" || calls[1] != "fn2" || calls[2] != "fn3" {
			t.Fatalf("got call order %v, want [fn1 fn2 fn3]", calls)
		}
		for i, a := range received {
			if a != alert {
				t.Errorf("call %d received %+v, want %+v", i, a, alert)
			}
		}
	})

	t.Run("no functions is a safe no-op", func(t *testing.T) {
		ChainAlert()(Alert{Topic: "t"}) // must not panic
	})
}
