package blkparser

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

type Block struct {
	Raw         []byte  `json:"-"`
	Hash        string  `json:"hash"` // 32 bytes
	Height      uint    `json:"height"`
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
	Parent      string  `json:"prev_block"` // 32 bytes
	Next        string  `json:"next_block"` // 32 bytes
}

func NewBlock(rawblock []byte) (block *Block, err error) {
	block = new(Block)
	// block.Raw = rawblock

	block.Hash = HashString(GetShaString(rawblock[:80]))
	block.Version = binary.LittleEndian.Uint32(rawblock[0:4])
	if !bytes.Equal(rawblock[4:36], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		block.Parent = HashString(rawblock[4:36])
	}
	block.MerkleRoot = hex.EncodeToString(rawblock[36:68])
	block.BlockTime = binary.LittleEndian.Uint32(rawblock[68:72])
	block.Bits = binary.LittleEndian.Uint32(rawblock[72:76])
	block.Nonce = binary.LittleEndian.Uint32(rawblock[76:80])
	block.Size = uint32(len(rawblock))

	// txs, _ := ParseTxs(rawblock[80:])
	// block.Txs = txs

	return
}
