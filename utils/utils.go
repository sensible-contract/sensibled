package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
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

func NewTxWit(txwitraw []byte) (wits []*model.TxWit, offset uint) {
	txWitcnt, txWitcntsize := DecodeVarIntForBlock(txwitraw[0:])
	offset = txWitcntsize

	wits = make([]*model.TxWit, txWitcnt)
	for witIndex := uint(0); witIndex < txWitcnt; witIndex++ {
		txWitScriptcnt, txWitScriptcntsize := DecodeVarIntForBlock(txwitraw[offset:])
		offset += txWitScriptcntsize

		txwit := new(model.TxWit)
		txwit.Script = txwitraw[offset : offset+txWitScriptcnt]

		wits[witIndex] = txwit
		offset += txWitScriptcnt
	}
	return
}

func EncodeTxNFT(tx *model.Tx) {
	for vin, input := range tx.TxIns {
		// 只支持第一个输入的NFT
		if vin != 0 {
			break
		}
		if len(input.ScriptWitness) == 0 {
			break
		}

		wits, offset := NewTxWit(input.ScriptWitness)
		if len(input.ScriptWitness) != int(offset) {
			break
		}

		// 只支持p2tr格式的见证，单NFT，多段OP_FALSE/OP_IF仅识别第一个。
		// 跳过没有脚本的wits
		if len(wits) < 2 {
			break
		}

		// 附件
		hasAnnex := (len(wits[len(wits)-1].Script) > 0 && wits[len(wits)-1].Script[0] == 0x50)
		// 跳过P2WPKH
		if len(wits) < 3 && hasAnnex {
			break
		}

		nftScript := wits[len(wits)-2].Script
		if hasAnnex {
			// fixme: -1 at official impliment
			nftScript = wits[len(wits)-3].Script
		}

		if nft, ok := scriptDecoder.ExtractPkScriptForNFT(nftScript); ok {
			nft.InTxVin = uint32(vin)
			if isBRC20(nft) {
				nft.IsBRC20 = true
			}

			input.CreatePointCountOfNewNFTs += 1
			tx.NewNFTDataCreated = append(tx.NewNFTDataCreated, nft)
			tx.GenesisNewNFT = true
		}
	}
}

type InscriptionNamePick struct {
	Proto     string `json:"p"`
	Operation string `json:"op"`

	BRC20Tick    string `json:"tick"` // brc20
	BRC20Max     string `json:"max"`  // brc20
	BRC20Limit   string `json:"lim"`  // brc20
	BRC20Amount  string `json:"amt"`  // brc20
	BRC20Decimal string `json:"dec"`  // brc20
}

func isBRC20(nft *scriptDecoder.NFTData) bool {
	if len(nft.ContentBody) < 40 {
		return false
	}

	if !bytes.Equal(nft.ContentType, []byte("text/plain")) &&
		!bytes.Equal(nft.ContentType, []byte("text/plain;charset=utf-8")) &&
		!bytes.Equal(nft.ContentType, []byte("text/plain;charset=UTF-8")) &&
		!bytes.Equal(nft.ContentType, []byte("application/json")) {
		if !bytes.HasPrefix(nft.ContentType, []byte("text/plain;")) {
			return false
		}
	}

	content := bytes.TrimSpace(nft.ContentBody)
	if !bytes.HasPrefix(content, []byte("{")) {
		return false
	}
	if !bytes.HasSuffix(content, []byte("}")) {
		return false
	}

	var namePick InscriptionNamePick
	if err := json.Unmarshal(nft.ContentBody, &namePick); err != nil {
		return false
	}
	if namePick.Proto != "brc-20" {
		return false
	}

	if namePick.BRC20Tick == "" {
		return false
	}

	return true
}

func GetWitnessHash256(data []byte, witOffset uint32) (hash []byte) {
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
