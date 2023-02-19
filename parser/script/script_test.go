package script

import (
	"encoding/hex"
	"testing"
)

func TestScript(t *testing.T) {

	scripts := []string{
		"0b3c4b616e7965323032303e7588",
		"6a3c4b616e7965323032303e0b00000000",
	}
	for line, scriptHex := range scripts {
		if len(scriptHex) == 0 {
			continue
		}
		_, err := hex.DecodeString(scriptHex)
		if err != nil {
			t.Logf("ignore line: %d", line)
			continue
		}
	}
}
