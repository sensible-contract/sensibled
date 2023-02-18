package serial

import (
	"unisatd/logger"
	"unisatd/mempool/store"
	"unisatd/model"
	"unisatd/utils"

	"go.uber.org/zap"
)

// SyncBlockNFT all nft in block height
func SyncBlockNFT(startIdx int, nfts []*model.NewInscriptionInfo) {
	for _, nft := range nfts {
		if _, err := store.SyncStmtNFT.Exec(
			string(nft.TxId),
			nft.IdxInTx,
			nft.NFTData.InTxVin,
			nft.InTxVout,
			nft.CreatePoint.Offset,
			string(nft.NFTData.ContentType),
			string(nft.NFTData.ContentBody),

			model.MEMPOOL_HEIGHT, // uint32(block.Height),
			nft.TxIdx,

			nft.CreatePoint.IdxInBlock,
			0,
		); err != nil {
			logger.Log.Info("sync-nft-err",
				zap.String("sync", "nft err"),
				zap.String("nftid", utils.HashString(nft.TxId)),
				zap.String("err", err.Error()),
			)
		}
	}
}