package serial

import (
	"unisatd/logger"
	"unisatd/model"
	"unisatd/store"

	"go.uber.org/zap"
)

// SyncBlock block id
func SyncBlock(block *model.Block) {
	coinbase := block.Txs[0]
	coinbaseOut := coinbase.OutputsValue

	txInputsValue := uint64(0)
	txOutputsValue := uint64(0)

	nftInputsCnt := uint64(0)
	nftOutputsCnt := uint64(0)

	// 在普通交易中丢弃，在coinbase中收集的的nft个数
	nftLostCnt := block.Txs[0].NFTInputsCnt

	nftNewCnt := uint64(0)

	for _, tx := range block.Txs[1:] {
		txInputsValue += tx.InputsValue
		txOutputsValue += tx.OutputsValue

		nftNewCnt += uint64(len(tx.CreateNFTData))
		nftInputsCnt += tx.NFTInputsCnt
		nftOutputsCnt += tx.NFTOutputsCnt
	}

	if _, err := store.SyncStmtBlk.Exec(
		uint32(block.Height),
		string(block.Hash),
		string(block.Parent),
		string(block.MerkleRoot),
		block.TxCnt,

		// nft
		nftNewCnt,
		nftInputsCnt,
		nftOutputsCnt,
		nftLostCnt,

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
