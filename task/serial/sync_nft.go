package serial

import (
	"unisatd/logger"
	"unisatd/model"
	"unisatd/store"
	"unisatd/utils"

	"go.uber.org/zap"
)

// SyncBlockNFT all nft in block height
func SyncBlockNFT(nfts []*model.NewInscriptionInfo) {
	for _, nft := range nfts {
		if _, err := store.SyncStmtNFT.Exec(
			string(nft.TxId),
			nft.IdxInTx,
			nft.NFTData.InTxVin,
			nft.InTxVout,
			nft.CreatePoint.Offset,
			string(nft.NFTData.ContentType),
			string(nft.NFTData.ContentBody),

			nft.CreatePoint.Height,
			nft.TxIdx,

			nft.CreatePoint.IdxInBlock,
			0, // fixme: missing number
		); err != nil {
			logger.Log.Info("sync-nft-err",
				zap.String("nftid", utils.HashString(nft.TxId)),
				zap.String("err", err.Error()),
			)
		}
	}
}
