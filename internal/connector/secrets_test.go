package connector

import "testing"

func TestRequireString(t *testing.T) {
	data := map[string]interface{}{
		"ok":    "value",
		"empty": "",
	}

	if _, err := requireString(data, "missing"); err == nil {
		t.Error("expected error for missing key")
	}
	if _, err := requireString(data, "empty"); err == nil {
		t.Error("expected error for empty value")
	}
	v, err := requireString(data, "ok")
	if err != nil || v != "value" {
		t.Errorf("got %q, %v", v, err)
	}
}
