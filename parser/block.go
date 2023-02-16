package parser

import (
	"encoding/binary"
	"unisatd/model"
	"unisatd/utils"
)

func NewBlock(rawblock []byte) (block *model.Block) {
	block = new(model.Block)
	InitBlock(block, rawblock)
	return block
}

func InitBlock(block *model.Block, rawblock []byte) {
	block.Raw = rawblock
	block.Hash = utils.GetHash256(rawblock[:80])
	block.HashHex = utils.HashString(block.Hash)
	block.Version = binary.LittleEndian.Uint32(rawblock[0:4])

	block.Parent = make([]byte, 32)
	copy(block.Parent, rawblock[4:36])
	block.ParentHex = utils.HashString(block.Parent)

	block.MerkleRoot = make([]byte, 32)
	copy(block.MerkleRoot, rawblock[36:68])

	block.BlockTime = binary.LittleEndian.Uint32(rawblock[68:72])
	block.Bits = binary.LittleEndian.Uint32(rawblock[72:76])
	block.Nonce = binary.LittleEndian.Uint32(rawblock[76:80])
	block.Size = uint32(len(rawblock))

	txcnt, _ := utils.DecodeVarIntForBlock(rawblock[80:])
	block.TxCnt = uint64(txcnt)
}
