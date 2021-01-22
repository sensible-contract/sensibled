package blkparser

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

func DecodeVariableLengthInteger(raw []byte) (cnt uint, cnt_size uint) {
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

func SafeDecodeVariableLengthInteger(raw []byte) (cnt uint, cnt_size uint) {
	if len(raw) < 1 {
		return 0, 0
	}
	if raw[0] < 0xfd {
		return uint(raw[0]), 1
	}

	if raw[0] == 0xfd {
		if len(raw) < 3 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint16(raw[1:3])), 3

	} else if raw[0] == 0xfe {
		if len(raw) < 5 {
			return 0, 0
		}
		return uint(binary.LittleEndian.Uint32(raw[1:5])), 5
	}

	if len(raw) < 9 {
		return 0, 0
	}
	return uint(binary.LittleEndian.Uint64(raw[1:9])), 9
}

// Get the Tx count, decode the variable length integer
// https://en.bitcoin.it/wiki/Protocol_specification#Variable_length_integer
func DecodeVariableLengthIntegerOrigin(raw []byte) (cnt int, cnt_size int) {
	if raw[0] < 0xfd {
		return int(raw[0]), 1
	}

	// if raw[0] == 0xfd {
	// 	return int(binary.LittleEndian.Uint16(raw[1:3])), 3
	// } else if raw[0] == 0xfe {
	// 	return int(binary.LittleEndian.Uint32(raw[1:5])), 5
	// } else {
	// 	return int(binary.LittleEndian.Uint64(raw[1:9])), 9
	// }

	cnt_size = 1 + (2 << (2 - (0xff - raw[0])))
	if len(raw) < 1+cnt_size {
		return
	}

	res := uint64(0)
	for i := 1; i < cnt_size; i++ {
		res |= (uint64(raw[i]) << uint64(8*(i-1)))
	}

	cnt = int(res)
	return
}

func GetShaString(data []byte) (hash []byte) {
	sha := sha256.New()
	sha.Write(data[:])
	tmp := sha.Sum(nil)
	sha.Reset()
	sha.Write(tmp)
	hash = sha.Sum(nil)
	return
}

func GetWitnessShaString(data []byte, witOffset uint) (hash []byte) {
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
