package script

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"
)

var scripts []string

func init() {
	dat, err := ioutil.ReadFile("test.txt")
	if err != nil {
		panic(err)
	}
	scripts = strings.Split(string(dat), "\n")
}

func TestDecode(t *testing.T) {
	for line, scriptHex := range scripts {
		if len(scriptHex) == 0 {
			continue
		}
		script, err := hex.DecodeString(scriptHex)
		if err != nil {
			t.Logf("ignore line: %d", line)
			continue
		}

		txo := &AddressData{}
		data, _ := json.Marshal(txo)
		t.Logf("scriptLen: %d, txo: %s", len(script), strings.ReplaceAll(string(data), ",", "\n"))
	}
}
