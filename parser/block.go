package parser

import (
	"blkparser/model"
	"blkparser/utils"
	"encoding/binary"
)

func NewBlock(rawblock []byte) (block *model.Block) {
	block = new(model.Block)
	block.Hash = utils.GetShaString(rawblock[:80])
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

	return block
}
