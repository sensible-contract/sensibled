package model

////////////////
type BlockDO struct {
	Height      uint32 `db:"height"`
	BlockId     []byte `db:"blkid"`
	PrevBlockId []byte `db:"previd"`
	MerkleRoot  []byte `db:"merkle"`
	TxCount     uint64 `db:"ntx"`
	BlockTime   uint32 `db:"blocktime"`
	Bits        uint32 `db:"bits"`
	BlockSize   uint32 `db:"blocksize"`
}
