package utils

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	} else if raw[0] == 0xfd {
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3
	} else if raw[0] == 0xfe {
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	} else {
		return uint(binary.LittleEndian.Uint64(raw[1:9])), 9
	}
}

func EncodeVarIntForBlock(cnt uint64, raw []byte) (cnt_size int) {
	if cnt < 0xfd {
		raw[0] = byte(cnt)
		return 1
	} else if cnt <= 0xffff {
		raw[0] = 0xfd
		binary.LittleEndian.PutUint16(raw[1:3], uint16(cnt))
		return 3
	} else if cnt <= 0xffffffff {
		raw[0] = 0xfe
		binary.LittleEndian.PutUint32(raw[1:5], uint32(cnt))
		return 5
	} else {
		raw[0] = 0xff
		binary.LittleEndian.PutUint64(raw[1:9], uint64(cnt))
		return 9
	}
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

func HashString(data []byte) (res string) {
	length := 32
	reverseData := make([]byte, length)

	// need reverse
	for i := 0; i < length; i++ {
		reverseData[i] = data[length-i-1]
	}

	return hex.EncodeToString(reverseData)
}
