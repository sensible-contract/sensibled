package serial

import (
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/store"

	"go.uber.org/zap"
)

// SyncBlockTxOutputInfo all tx output info
func SyncBlockTxOutputInfo(block *model.Block) {
	for txIdx, tx := range block.Txs {
		for _, output := range tx.TxOuts {
			tx.NFTOutputsCnt += uint64(len(output.CreatePointOfNFTs))
			tx.OutputsValue += output.Satoshi
			// set sensible flag
			if output.AddressData.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}
		}

		for vout, output := range tx.TxOuts {
			// prune false opreturn output
			if isOpReturnPrune && !tx.IsSensible && scriptDecoder.IsOpreturn(output.ScriptType) {
				continue
			}

			// prune string(output.Pkscript),
			pkscript := ""
			if !isPkScriptPrune || tx.IsSensible || output.AddressData.HasAddress {
				pkscript = string(output.PkScript)
			}

			address := ""
			if output.AddressData.HasAddress {
				address = string(output.AddressData.AddressPkh[:]) // 20 bytes
			}

			var dataValue uint64
			if output.AddressData.CodeType == scriptDecoder.CodeType_NFT {
			}

			nftPointsBuf := make([]byte, len(output.CreatePointOfNFTs)*3*8)
			model.DumpNFTCreatePoints(nftPointsBuf, output.CreatePointOfNFTs)

			if _, err := store.SyncStmtTxOut.Exec(
				string(tx.TxId),
				uint32(vout),
				address,
				uint32(output.AddressData.CodeType),
				dataValue,
				output.Satoshi,
				string(output.ScriptType),
				pkscript,

				// nft
				uint64(len(output.CreatePointOfNFTs)),
				string(nftPointsBuf),

				uint32(block.Height),
				uint64(txIdx),
			); err != nil {
				logger.Log.Info("sync-txout-err",
					zap.String("sync", "txout err"),
					zap.String("utxid", tx.TxIdHex),
					zap.Uint32("vout", uint32(vout)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
