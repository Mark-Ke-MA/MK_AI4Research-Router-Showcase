package appserver

import (
	"fmt"
	"testing"
)

func TestThreadTurnCorrelation(t *testing.T) {
	c := NewCorrelator("wanted-thread", "wanted-turn")
	wrong := []byte(`{"method":"item/completed","params":{"threadId":"other-thread","turnId":"wanted-turn","item":{"type":"agentMessage","text":"wrong"}}}`)
	done, text, err := c.Observe(wrong)
	if err != nil || done || text != "" {
		t.Fatal("accepted wrong thread")
	}
	item := []byte(`{"method":"item/completed","params":{"threadId":"wanted-thread","turnId":"wanted-turn","item":{"type":"agentMessage","text":"ok"}}}`)
	if done, _, err = c.Observe(item); err != nil || done {
		t.Fatal()
	}
	turn := []byte(`{"method":"turn/completed","params":{"threadId":"wanted-thread","turn":{"id":"wanted-turn","status":"completed"}}}`)
	done, text, err = c.Observe(turn)
	if err != nil || !done || text != "ok" {
		t.Fatalf("%v %v %q", done, err, text)
	}
}
func TestRejectsCrossTurnCompletion(t *testing.T) {
	c := NewCorrelator("t", "u")
	raw := []byte(fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":"t","turn":{"id":"%s"}}}`, "other"))
	done, _, _ := c.Observe(raw)
	if done {
		t.Fatal("cross turn accepted")
	}
}
