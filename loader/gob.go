package loader

import (
	"encoding/gob"
	"os"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/utils"

	"go.uber.org/zap"
)

func LoadFromGobFile(fname string, data map[string]*model.Block) (lastFileIdx int) {
	logger.Log.Info("loading gob...")
	gobFile, err := os.Open(fname)
	if err != nil {
		logger.Log.Error("open gob gob failed", zap.Error(err))
		return
	}
	gobDec := gob.NewDecoder(gobFile)

	cacheData := []model.BlockCache{}
	if err := gobDec.Decode(&cacheData); err != nil {
		logger.Log.Info("load gob failed", zap.Error(err))
	}

	maxFileIdx := 0
	for _, blk := range cacheData {
		if blk.FileIdx > maxFileIdx {
			maxFileIdx = blk.FileIdx
		}
		hashHex := utils.HashString(blk.Hash)
		data[hashHex] = &model.Block{
			Hash:       blk.Hash,
			HashHex:    hashHex,
			TxCnt:      blk.TxCnt,
			FileIdx:    blk.FileIdx,
			FileOffset: blk.FileOffset,
			Parent:     blk.Parent,
			ParentHex:  utils.HashString(blk.Parent),
		}
	}
	return maxFileIdx
}

func DumpToGobFile(fname string, data map[string]*model.Block) {
	cacheData := make([]model.BlockCache, len(data))
	idx := 0
	for _, blk := range data {
		bc := model.BlockCache{
			Height:     blk.Height,
			Hash:       blk.Hash,
			TxCnt:      blk.TxCnt,
			FileIdx:    blk.FileIdx,
			FileOffset: blk.FileOffset,
			Parent:     blk.Parent,
		}
		cacheData[idx] = bc
		idx++
	}

	gobFile, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		logger.Log.Error("dump gob file failed", zap.Error(err))
		return
	}
	defer gobFile.Close()

	enc := gob.NewEncoder(gobFile)
	if err := enc.Encode(cacheData); err != nil {
		logger.Log.Error("dump gob failed", zap.Error(err))
	}
	logger.Log.Info("dump gob ok")
}
