package serial

import (
	"sensibled/logger"
	"sensibled/model"
	"sensibled/store"

	"go.uber.org/zap"
)

// SyncBlock block id
func SyncBlock(block *model.Block) {
	coinbase := block.Txs[0]
	coinbaseOut := coinbase.OutputsValue

	txInputsValue := uint64(0)
	txOutputsValue := uint64(0)

	for _, tx := range block.Txs[1:] {
		txInputsValue += tx.InputsValue
		txOutputsValue += tx.OutputsValue
	}

	for _, tokenSummary := range block.ParseData.TokenSummaryMap {
		if _, err := store.SyncStmtBlkCodeHash.Exec(
			uint32(block.Height),
			string(tokenSummary.CodeHash),
			string(tokenSummary.GenesisId),
			uint32(tokenSummary.CodeType),
			tokenSummary.NFTIdx,
			tokenSummary.InDataValue,
			tokenSummary.OutDataValue,
			tokenSummary.InSatoshi,
			tokenSummary.OutSatoshi,
			string(block.Hash),
		); err != nil {
			logger.Log.Info("sync-block-codehash-err",
				zap.String("blkid", block.HashHex),
				zap.String("err", err.Error()),
			)
		}
	}

	if _, err := store.SyncStmtBlk.Exec(
		uint32(block.Height),
		string(block.Hash),
		string(block.Parent),
		string(block.MerkleRoot),
		block.TxCnt,
		txInputsValue,
		txOutputsValue,
		coinbaseOut,
		block.BlockTime,
		block.Bits,
		block.Size,
	); err != nil {
		logger.Log.Info("sync-block-err",
			zap.String("blkid", block.HashHex),
			zap.String("err", err.Error()),
		)
	}
}
