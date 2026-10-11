package adapters

import (
	"codex-persona-router/internal/model"
	"errors"
	"testing"
)

func TestOwnerPersonaAndEndpointFailClosed(t *testing.T) {
	p := Policy{Persona: "p", Owner: "owner", AllowedPlatforms: map[string]bool{"telegram": true}}
	base := Event{Persona: "p", Platform: "telegram", EventKey: "e", Account: "a", Conversation: "c", Owner: "owner", Text: "hello"}
	if _, err := Normalize(p, base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.Owner = "intruder"
	if _, err := Normalize(p, bad); !errors.Is(err, model.ErrEndpoint) {
		t.Fatal("owner accepted")
	}
	bad = base
	bad.Persona = "other"
	if _, err := Normalize(p, bad); !errors.Is(err, model.ErrCrossPersona) {
		t.Fatal("persona accepted")
	}
	bad = base
	bad.Platform = "feishu"
	if _, err := Normalize(p, bad); !errors.Is(err, model.ErrEndpoint) {
		t.Fatal("platform accepted")
	}
}
func TestMediaAndLengthValidation(t *testing.T) {
	p := Policy{Persona: "p", Owner: "o", AllowedPlatforms: map[string]bool{"feishu": true}}
	e := Event{Persona: "p", Platform: "feishu", EventKey: "e", Account: "a", Conversation: "c", Owner: "o", Text: string(make([]byte, MaxTextBytes+9))}
	in, err := Normalize(p, e)
	if err != nil || string(in.Payload) != OversizeTextNotice {
		t.Fatal("oversize input was silently truncated")
	}
	e.Media = []MediaRef{{Kind: "file", OpaqueKey: "x", Size: 1}}
	if _, err := Normalize(p, e); err == nil {
		t.Fatal("unsupported media accepted")
	}
}
