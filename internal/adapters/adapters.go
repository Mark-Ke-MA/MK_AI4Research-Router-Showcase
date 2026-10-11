package adapters

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"codex-persona-router/internal/model"
)

const (
	MaxTextBytes = 24000
	// OversizeTextNotice replaces an over-limit bridged message atomically.
	// The original body is neither truncated nor persisted, and the canonical
	// agent can tell the owner to resend it through an appropriate surface.
	OversizeTextNotice = "[bridge: source message exceeded 24000 bytes and was not forwarded; ask the user to shorten it or use Codex GUI]"
)

type Event struct {
	Persona      string
	Platform     string
	EventKey     string
	Account      string
	Conversation string
	Topic        string
	Owner        string
	Text         string
	Media        []MediaRef
}
type MediaRef struct {
	Kind, OpaqueKey, MIME, LocalPath string
	Size                             int64
}
type Policy struct {
	Persona, Owner   string
	AllowedPlatforms map[string]bool
}

func Normalize(p Policy, e Event) (model.Inbound, error) {
	if e.Persona != p.Persona {
		return model.Inbound{}, model.ErrCrossPersona
	}
	if !p.AllowedPlatforms[e.Platform] || e.Owner == "" || e.Owner != p.Owner {
		return model.Inbound{}, model.ErrEndpoint
	}
	if e.EventKey == "" || e.Account == "" || e.Conversation == "" {
		return model.Inbound{}, model.ErrEndpoint
	}
	text := strings.TrimSpace(e.Text)
	if text == "" && len(e.Media) == 0 {
		return model.Inbound{}, fmt.Errorf("empty event")
	}
	if len(text) > MaxTextBytes {
		text = OversizeTextNotice
	}
	var mediaPaths []string
	for _, m := range e.Media {
		if m.OpaqueKey == "" || m.Size < 0 || m.Size > 20<<20 {
			return model.Inbound{}, fmt.Errorf("invalid media")
		}
		if m.Kind != "image" {
			return model.Inbound{}, fmt.Errorf("unsupported media")
		}
		if m.LocalPath != "" {
			if !filepath.IsAbs(m.LocalPath) || filepath.Clean(m.LocalPath) != m.LocalPath {
				return model.Inbound{}, fmt.Errorf("unsafe media path")
			}
			mediaPaths = append(mediaPaths, m.LocalPath)
		}
	}
	target, err := json.Marshal(struct{ Account, Conversation, Topic string }{e.Account, e.Conversation, e.Topic})
	if err != nil {
		return model.Inbound{}, fmt.Errorf("invalid delivery target")
	}
	return model.Inbound{Platform: e.Platform, EventKey: e.EventKey, Account: e.Account, Owner: e.Owner, Conversation: e.Conversation, Topic: e.Topic, Payload: []byte(text), DeliveryTarget: target, MediaPaths: mediaPaths}, nil
}
