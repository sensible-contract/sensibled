package script

import (
	"bytes"
)

func hasSensibleFlag(pkScript []byte) bool {
	return bytes.HasSuffix(pkScript, []byte("sensible")) || bytes.HasSuffix(pkScript, []byte("oraclesv"))
}

func DecodeSensibleTxo(pkScript []byte, txo *TxoData) bool {
	scriptLen := len(pkScript)
	if scriptLen < 1024 {
		return false
	}

	ret := false
	return ret
}

func ExtractPkScriptForTxo(pkScript, scriptType []byte) (txo *TxoData) {
	txo = &TxoData{}

	if len(pkScript) == 0 {
		return txo
	}

	if isPubkeyHash(scriptType) {
		txo.HasAddress = true
		copy(txo.AddressPkh[:], pkScript[3:23])
		return txo
	}

	if isPayToScriptHash(scriptType) {
		txo.HasAddress = true
		copy(txo.AddressPkh[:], GetHash160(pkScript[2:len(pkScript)-1]))
		return txo
	}

	if isPubkey(scriptType) {
		txo.HasAddress = true
		copy(txo.AddressPkh[:], GetHash160(pkScript[1:len(pkScript)-1]))
		return txo
	}

	// if isMultiSig(scriptType) {
	// 	return pkScript[:]
	// }

	if IsOpreturn(scriptType) {
		if hasSensibleFlag(pkScript) {
			txo.CodeType = CodeType_SENSIBLE
		}
		return txo
	}

	DecodeSensibleTxo(pkScript, txo)

	return txo
}

func GetLockingScriptType(pkScript []byte) (scriptType []byte) {
	length := len(pkScript)
	if length == 0 {
		return
	}
	scriptType = make([]byte, 0)

	lenType := 0
	p := uint(0)
	e := uint(length)

	for p < e && lenType < 32 {
		c := pkScript[p]
		if 0 < c && c < 0x4f {
			cnt, cntsize := SafeDecodeVarIntForScript(pkScript[p:])
			p += cnt + cntsize
		} else {
			p += 1
		}
		scriptType = append(scriptType, c)
		lenType += 1
	}
	return
}
