package parallel

import (
	"blkparser/utils"
)

func isLockingScriptOnlyEqual(pkscript []byte) bool {
	// test locking script
	// "0b 3c4b616e7965323032303e 87"

	length := len(pkscript)
	if length == 0 {
		return true
	}
	if pkscript[length-1] != 0x87 {
		return false
	}
	cnt, cntsize := utils.SafeDecodeVarIntForScript(pkscript)
	if length == int(cnt+cntsize+1) {
		return true
	}
	return false
}

func GetLockingScriptType(pkscript []byte) (scriptType []byte) {
	length := len(pkscript)
	if length == 0 {
		return
	}
	scriptType = make([]byte, 0)

	lenType := 0
	p := uint(0)
	e := uint(length)

	for p < e && lenType < 32 {
		c := pkscript[p]
		if 0 < c && c < 0x4f {
			cnt, cntsize := utils.SafeDecodeVarIntForScript(pkscript[p:])
			p += cnt + cntsize
		} else {
			p += 1
		}
		scriptType = append(scriptType, c)
		lenType += 1
	}
	return
}
