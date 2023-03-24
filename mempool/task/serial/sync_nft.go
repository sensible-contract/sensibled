package serial

import (
	"unisatd/logger"
	"unisatd/mempool/store"
	"unisatd/model"
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

			nft.Satoshi,
			string(nft.PkScript),
			nft.InputsValue,
			nft.OutputsValue,

			string(nft.NFTData.ContentType),
			uint32(len(nft.NFTData.ContentBody)),
			string(nft.NFTData.ContentBody),

			nft.CreatePoint.Height,
			nft.TxIdx,
			nft.BlockTime,

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

// SyncBlockBRC20 all brc20 in block height
func SyncBlockBRC20(brc20s []*model.NewInscriptionInfo) {
	for _, brc20 := range brc20s {
		if _, err := store.SyncStmtBRC20.Exec(
			string(brc20.TxId),
			brc20.IdxInTx,
			brc20.NFTData.InTxVin,
			brc20.InTxVout,
			brc20.CreatePoint.Offset,

			brc20.Satoshi,
			string(brc20.PkScript),
			brc20.InputsValue,
			brc20.OutputsValue,

			string(brc20.NFTData.ContentType),
			uint32(len(brc20.NFTData.ContentBody)),
			string(brc20.NFTData.ContentBody),

			brc20.CreatePoint.Height,
			brc20.TxIdx,
			brc20.BlockTime,

			brc20.CreatePoint.IdxInBlock,
			brc20.Number,
			brc20.Height,
		); err != nil {
			logger.Log.Info("sync-brc20-err",
				zap.String("brc20id", utils.HashString(brc20.TxId)),
				zap.String("err", err.Error()),
			)
		}
	}
}
