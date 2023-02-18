package script

import (
	"crypto/sha256"
	"encoding/binary"

	"golang.org/x/crypto/ripemd160"
)

func GetHash160(data []byte) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:])
	tmp := sha.Sum(nil)

	rp := ripemd160.New()
	rp.Write(tmp)
	hash = rp.Sum(nil)
	return
}

func SafeDecodeVarIntForScript(raw []byte) (cnt uint, cnt_size uint) {
	if len(raw) < 1 {
		return 0, 0
	}
	if raw[0] < OP_PUSHDATA1 {
		return uint(raw[0]), 1
	}

	if raw[0] == OP_PUSHDATA1 {
		if len(raw) < 2 {
			return 0, 0
		}
		return uint(raw[1]), 2

	} else if raw[0] == OP_PUSHDATA2 {
		if len(raw) < 3 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3

	} else if raw[0] == OP_PUSHDATA4 {
		if len(raw) < 5 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	}

	return 0, 0
}

func GetOpcodeFormScript(raw []byte) (size uint, data []byte, isPush bool) {
	if len(raw) < 1 {
		return 0, nil, false
	}

	c := raw[0]
	if c > OP_16 {
		return 1, raw[0:1], false
	}
	// skip valid tag
	if 0 < c && c < 0x4f {
		cnt, cntsize := SafeDecodeVarIntForScript(raw)
		if int(cntsize+cnt) == 0 || int(cntsize+cnt) > len(raw) {
			return cntsize + cnt, nil, false
		}
		return cntsize + cnt, raw[cntsize : cntsize+cnt], true
	} else {
		return 1, raw[0:1], true
	}
}

func getVarIntLen(length int) int {
	res := 0
	if length <= 0x4b {
		res = 0
	} else if length <= 0xff {
		res = 1
	} else if length <= 0xffff {
		res = 2
	} else {
		res = 4
	}
	return res
}

func ReverseBytesInPlace(data []byte) {
	n := len(data)
	for i := 0; i < n/2; i++ {
		data[i], data[n-1-i] = data[n-1-i], data[i]
	}
}
