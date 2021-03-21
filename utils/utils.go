package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"golang.org/x/crypto/ripemd160"
)

func CalcBlockSubsidy(height int) uint64 {
	var SubsidyReductionInterval = 210000
	var SatoshiPerBitcoin uint64 = 100000000
	var baseSubsidy = 50 * SatoshiPerBitcoin
	// Equivalent to: baseSubsidy / 2^(height/subsidyHalvingInterval)
	return baseSubsidy >> uint(height/SubsidyReductionInterval)
}

func DecodeVarIntForBlock(raw []byte) (cnt uint, cnt_size uint) {
	if raw[0] < 0xfd {
		return uint(raw[0]), 1
	}

	if raw[0] == 0xfd {
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3
	} else if raw[0] == 0xfe {
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	}
	return uint(binary.LittleEndian.Uint64(raw[1:9])), 9
}

func SafeDecodeVarIntForScript(raw []byte) (cnt uint, cnt_size uint) {
	if len(raw) < 1 {
		return 0, 0
	}
	if raw[0] < 0x4c {
		return uint(raw[0]), 1
	}

	if raw[0] == 0x4c {
		if len(raw) < 2 {
			return 0, 0
		}
		return uint(raw[1]), 2

	} else if raw[0] == 0x4d {
		if len(raw) < 3 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3

	} else if raw[0] == 0x4e {
		if len(raw) < 5 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	}

	return 0, 0
}

func GetHash256(data []byte) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:])
	tmp := sha.Sum(nil)
	sha.Reset()
	sha.Write(tmp)
	hash = sha.Sum(nil)
	return
}

func GetHash160(data []byte) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:])
	tmp := sha.Sum(nil)

	rp := ripemd160.New()
	rp.Write(tmp)
	hash = rp.Sum(nil)
	return
}

func GetWitnessHash256(data []byte, witOffset uint) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:4]) // version
	// skip 2 bytes
	sha.Write(data[4+2 : witOffset]) // inputs/outputs
	// skip witness
	sha.Write(data[len(data)-4:]) // locktime
	tmp := sha.Sum(nil)
	sha.Reset()
	sha.Write(tmp)
	hash = sha.Sum(nil)
	return
}

func HashString(data []byte) (res string) {
	length := 32
	reverseData := make([]byte, length)

	// need reverse
	for i := 0; i < length; i++ {
		reverseData[i] = data[length-i-1]
	}

	return hex.EncodeToString(reverseData)
}
