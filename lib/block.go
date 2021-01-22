package blkparser

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

type Block struct {
	Raw         []byte  `json:"-"`
	HashHex     string  `json:"hash"` // 32 bytes
	FileIdx     int     `json:"file_idx"`
	FileOffset  int     `json:"file_offset"`
	Height      int     `json:"height"`
	Txs         []*Tx   `json:"tx,omitempty"`
	Version     uint32  `json:"ver"`
	MerkleRoot  string  `json:"mrkl_root"`
	BlockTime   uint32  `json:"time"`
	Bits        uint32  `json:"bits"`
	Nonce       uint32  `json:"nonce"`
	Size        uint32  `json:"size"`
	TxCnt       uint32  `json:"n_tx"`
	TotalBTC    uint64  `json:"total_out"`
	BlockReward float64 `json:"-"`
	ParentHex   string  `json:"prev_block"` // 32 bytes
	NextHex     string  `json:"next_block"` // 32 bytes
}

func NewBlock(rawblock []byte) (block *Block) {
	block = new(Block)

	block.HashHex = HashString(GetShaString(rawblock[:80]))
	block.Version = binary.LittleEndian.Uint32(rawblock[0:4])
	if !bytes.Equal(rawblock[4:36], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		block.ParentHex = HashString(rawblock[4:36])
	}
	block.MerkleRoot = hex.EncodeToString(rawblock[36:68])
	block.BlockTime = binary.LittleEndian.Uint32(rawblock[68:72])
	block.Bits = binary.LittleEndian.Uint32(rawblock[72:76])
	block.Nonce = binary.LittleEndian.Uint32(rawblock[76:80])
	block.Size = uint32(len(rawblock))

	return block
}

func ParseTxs(txsraw []byte) (txs []*Tx) {
	offset := uint(0)
	txcnt, txcnt_size := DecodeVariableLengthInteger(txsraw[offset:])
	offset += txcnt_size

	txs = make([]*Tx, txcnt)

	txoffset := uint(0)
	for i := range txs {
		txs[i], txoffset = NewTx(txsraw[offset:])
		if txs[i].WitOffset > 0 {
			txs[i].Hash = GetWitnessShaString(txsraw[offset:offset+txoffset], txs[i].WitOffset)
			txs[i].HashHex = HashString(txs[i].Hash)
		} else {
			txs[i].Hash = GetShaString(txsraw[offset : offset+txoffset])
			txs[i].HashHex = HashString(txs[i].Hash)
		}

		txs[i].Size = uint32(txoffset)
		offset += txoffset

		// other init:
		initTx(txs[i])
	}
	return txs
}
