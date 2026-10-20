package mailbox

import (
	"encoding/base64"
	"encoding/json"
)

type stageWire struct {
	Slot string `json:"slot"`
	Body string `json:"body"`
}

type pullWire struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

// DecodeStage parses {"slot":"<64 hex>","body":"<standard base64>"}.
func DecodeStage(body []byte) (Slot, Ciphertext, error) {
	var in stageWire
	if err := json.Unmarshal(body, &in); err != nil {
		return Slot{}, Ciphertext{}, ErrInvalid
	}
	slot, err := ParseSlot(in.Slot)
	if err != nil {
		return Slot{}, Ciphertext{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(in.Body)
	if err != nil {
		return Slot{}, Ciphertext{}, ErrInvalid
	}
	if len(raw) < headerLen || len(raw) > MaxBytes {
		return Slot{}, Ciphertext{}, ErrTooBig
	}
	return slot, Ciphertext{raw: raw}, nil
}

func EncodeStage(slot Slot, body Ciphertext) ([]byte, error) {
	if len(body.raw) < headerLen || len(body.raw) > MaxBytes {
		return nil, ErrTooBig
	}
	return json.Marshal(stageWire{
		Slot: slot.String(),
		Body: base64.StdEncoding.EncodeToString(body.raw),
	})
}

// EncodePull is a JSON array of {"id","body"}.
// The array has no sender and no plaintext.
func EncodePull(rows []Stored) ([]byte, error) {
	out := make([]pullWire, 0, len(rows))
	for _, row := range rows {
		out = append(out, pullWire{
			ID:   row.ID.String(),
			Body: base64.StdEncoding.EncodeToString(row.Body.raw),
		})
	}
	return json.Marshal(out)
}

func DecodePull(body []byte) ([]Stored, error) {
	var items []pullWire
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, ErrInvalid
	}
	out := make([]Stored, 0, len(items))
	for _, it := range items {
		id, err := ParseDropID(it.ID)
		if err != nil {
			return nil, err
		}
		raw, err := base64.StdEncoding.DecodeString(it.Body)
		if err != nil {
			return nil, ErrInvalid
		}
		if len(raw) < headerLen || len(raw) > MaxBytes {
			return nil, ErrTooBig
		}
		out = append(out, Stored{ID: id, Body: Ciphertext{raw: raw}})
	}
	return out, nil
}
