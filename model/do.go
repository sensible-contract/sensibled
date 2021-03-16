package model

////////////////
type BlockDO struct {
	Height  uint32 `db:"height"`
	BlockId []byte `db:"blkid"`
}
