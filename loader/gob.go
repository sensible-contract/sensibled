package loader

import (
	"blkparser/logger"
	"blkparser/model"
	"blkparser/utils"
	"encoding/gob"
	"os"

	"go.uber.org/zap"
)

func LoadFromGobFile(fname string, data map[string]*model.Block) {
	gobFile, err := os.Open(fname)
	if err != nil {
		logger.LogErr.Info("load gob",
			zap.String("log", "open gob gob failed"),
		)
		return
	}
	gobDec := gob.NewDecoder(gobFile)
	logger.LogErr.Info("load gob",
		zap.String("log", "loading gob"),
	)

	cacheData := []model.BlockCache{}
	if err := gobDec.Decode(&cacheData); err != nil {
		logger.LogErr.Info("load gob",
			zap.String("log", "load gob failed"),
			zap.String("err", err.Error()),
		)
	}
	for _, blk := range cacheData {
		hashHex := utils.HashString(blk.Hash)
		data[hashHex] = &model.Block{
			Hash:       blk.Hash,
			HashHex:    hashHex,
			FileIdx:    blk.FileIdx,
			FileOffset: blk.FileOffset,
			Parent:     blk.Parent,
			ParentHex:  utils.HashString(blk.Parent),
		}
	}
}

func DumpToGobFile(fname string, data map[string]*model.Block) {
	cacheData := []model.BlockCache{}
	for _, blk := range data {
		cacheData = append(cacheData, model.BlockCache{
			Hash:       blk.Hash,
			FileIdx:    blk.FileIdx,
			FileOffset: blk.FileOffset,
			Parent:     blk.Parent,
		})
	}

	gobFile, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		logger.LogErr.Info("dump gob",
			zap.String("log", "dump gob file failed"),
		)
		return
	}
	defer gobFile.Close()

	enc := gob.NewEncoder(gobFile)
	if err := enc.Encode(cacheData); err != nil {
		logger.LogErr.Info("dump gob",
			zap.String("log", "dump gob failed"),
		)
	}
	logger.LogErr.Info("dump gob",
		zap.String("log", "dump gob ok"),
	)
}
