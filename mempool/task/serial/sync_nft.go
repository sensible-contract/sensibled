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
			uint32(len(nft.NFTData.ContentBody)),
			string(nft.NFTData.ContentBody),

			model.MEMPOOL_HEIGHT,
			nft.TxIdx,

			nft.CreatePoint.IdxInBlock,
			nft.Number,
		); err != nil {
			logger.Log.Info("sync-nft-err",
				zap.String("sync", "nft err"),
				zap.String("nftid", utils.HashString(nft.TxId)),
				zap.String("err", err.Error()),
			)
		}
	}
}
