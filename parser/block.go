package parser

import (
	"blkparser/model"
	"blkparser/utils"
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

func NewBlock(rawblock []byte) (block *model.Block) {
	block = new(model.Block)

	block.HashHex = utils.HashString(utils.GetShaString(rawblock[:80]))
	block.Version = binary.LittleEndian.Uint32(rawblock[0:4])
	if !bytes.Equal(rawblock[4:36], []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		block.ParentHex = utils.HashString(rawblock[4:36])
	}
	block.MerkleRoot = hex.EncodeToString(rawblock[36:68])
	block.BlockTime = binary.LittleEndian.Uint32(rawblock[68:72])
	block.Bits = binary.LittleEndian.Uint32(rawblock[72:76])
	block.Nonce = binary.LittleEndian.Uint32(rawblock[76:80])
	block.Size = uint32(len(rawblock))

	return block
}
