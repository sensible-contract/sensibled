package script

import "encoding/binary"

// asSmallInt returns the passed opcode, which must be true according to
// isSmallInt(), as an integer.
func asSmallInt(op byte) int {
	if op == OP_0 {
		return 0
	}

	return int(op - (OP_1 - 1))
}

// isSmallInt returns whether or not the opcode is considered a small integer,
// which is an OP_0, or OP_1 through OP_16.
func isSmallInt(op byte) bool {
	if op == OP_0 || (op >= OP_1 && op <= OP_16) {
		return true
	}
	return false
}

// isPubkey returns true if the script passed is a pay-to-pubkey transaction,
// false otherwise.
func isPubkey(scriptType []byte) bool {
	// Valid pubkeys are either 33 or 65 bytes.
	return len(scriptType) == 2 &&
		(scriptType[0] == 33 || scriptType[0] == 65) &&
		scriptType[1] == OP_CHECKSIG
}

// Recent output script type, pays to hash160(script)
func isPayToScriptHash(scriptType []byte) bool {
	return len(scriptType) == 3 &&
		scriptType[0] == OP_HASH160 &&
		scriptType[1] == OP_DATA_20 &&
		scriptType[2] == OP_EQUAL
}

// isPubkeyHash returns true if the script passed is a pay-to-pubkey-hash
// transaction, false otherwise.
func isPubkeyHash(scriptType []byte) bool {
	return len(scriptType) == 5 &&
		scriptType[0] == OP_DUP &&
		scriptType[1] == OP_HASH160 &&
		scriptType[2] == OP_DATA_20 &&
		scriptType[3] == OP_EQUALVERIFY &&
		scriptType[4] == OP_CHECKSIG
}

// isMultiSig returns true if the passed script is a multisig transaction, false
// otherwise.
func isMultiSig(scriptType []byte) bool {
	// The absolute minimum is 1 pubkey:
	// OP_0/OP_1-16 <pubkey> OP_1 OP_CHECKMULTISIG
	l := len(scriptType)
	if l < 4 {
		return false
	}
	if !isSmallInt(scriptType[0]) {
		return false
	}
	if !isSmallInt(scriptType[l-2]) {
		return false
	}
	if scriptType[l-1] != OP_CHECKMULTISIG {
		return false
	}

	// Verify the number of pubkeys specified matches the actual number
	// of pubkeys provided.
	if l-2-1 != asSmallInt(scriptType[l-2]) {
		return false
	}

	for _, pop := range scriptType[1 : l-2] {
		// Valid pubkeys are either 33 or 65 bytes.
		if pop != 33 && pop != 65 {
			return false
		}
	}
	return true
}

func IsOpreturn(scriptType []byte) bool {
	if len(scriptType) > 0 && scriptType[0] == OP_RETURN {
		return true
	}
	if len(scriptType) > 1 && scriptType[0] == OP_FALSE && scriptType[1] == OP_RETURN {
		return true
	}
	return false
}

func IsFalseOpreturn(scriptType []byte) bool {
	if len(scriptType) > 1 && scriptType[0] == OP_FALSE && scriptType[1] == OP_RETURN {
		return true
	}
	return false
}

func IsLockingScriptOnlyEqual(pkScript []byte) bool {
	// test locking script
	// "0b 3c4b616e7965323032303e 87"

	length := len(pkScript)
	if length == 0 {
		return true
	}
	if pkScript[length-1] != OP_EQUAL {
		return false
	}
	cnt, cntsize := SafeDecodeVarIntForScript(pkScript)
	if length == int(cnt+cntsize+1) {
		return true
	}
	return false
}

func GetLockingScriptPushDropPosition(pkScript []byte) (pc int, ok bool) {
	// test locking script
	// "0b 3c4b616e7965323032303e 75"

	length := len(pkScript)
	if length == 0 {
		return 0, false
	}

	if pkScript[0] > OP_16 {
		return 0, false
	}

	cnt, cntsize := SafeDecodeVarIntForScript(pkScript)
	pc = int(cnt + cntsize)
	if length < pc+1 {
		return 0, false
	}

	if pkScript[pc] != OP_DROP {
		return 0, false
	}
	return pc + 1, true
}

func GetLockingScriptStatePosition(pkScript []byte) (pc int, ok bool) {
	// test locking script
	// "6a 3c4b616e7965323032303e 0b000000 00"

	length := len(pkScript)
	// opreturn + state + stateLen + version
	if length < 1+0+4+1 {
		return 0, false
	}

	stateLen := binary.LittleEndian.Uint32(pkScript[length-1-4 : length-1])
	pc = length - int(1+stateLen+4+1) // postion opreturn
	if pc < 0 {
		return 0, false
	}

	if pkScript[pc] != OP_RETURN {
		return 0, false
	}

	return pc + 1, true
}
