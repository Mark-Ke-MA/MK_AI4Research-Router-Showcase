package appserver

import (
	"encoding/json"
	"fmt"
)

type Notification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type turnParams struct {
	ThreadID string `json:"threadId"`
	Turn     struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"turn"`
}
type itemParams struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
	Item     struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"item"`
}

type Correlator struct{ threadID, turnID, text string }

func NewCorrelator(threadID, turnID string) *Correlator {
	return &Correlator{threadID: threadID, turnID: turnID}
}
func (c *Correlator) Observe(raw []byte) (bool, string, error) {
	var n Notification
	if err := json.Unmarshal(raw, &n); err != nil {
		return false, "", fmt.Errorf("invalid notification")
	}
	switch n.Method {
	case "item/completed":
		var p itemParams
		if json.Unmarshal(n.Params, &p) != nil {
			return false, "", fmt.Errorf("invalid item")
		}
		if p.ThreadID == c.threadID && p.TurnID == c.turnID && p.Item.Type == "agentMessage" {
			c.text = p.Item.Text
		}
	case "turn/completed":
		var p turnParams
		if json.Unmarshal(n.Params, &p) != nil {
			return false, "", fmt.Errorf("invalid turn")
		}
		if p.ThreadID == c.threadID && p.Turn.ID == c.turnID {
			return true, c.text, nil
		}
	}
	return false, "", nil
}

type ExternalTurnObserver interface {
	Started(threadID, turnID string)
	Completed(threadID, turnID string)
}
