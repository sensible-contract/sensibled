package serial

import (
	"context"
	"fmt"
	"unisatd/logger"
	"unisatd/model"
	"unisatd/utils"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// ParseMempoolBatchTxNFTsInAndOutSerial all tx input/output info
func ParseMempoolBatchTxNFTsInAndOutSerial(startIdx int, nftIndexInBlock uint64, txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData) (newInscriptions []*model.NewInscriptionInfo) {

	for txIdx, tx := range txs {
		// invalid exist nft recreate
		satInputOffset := uint64(0)
		for vin, input := range tx.TxIns {
			objData, ok := mpSpentUtxo[input.InputOutpointKey]
			if !ok {
				logger.Log.Info("tx-input-err",
					zap.String("txin", "input missing utxo"),
					zap.String("txid", tx.TxIdHex),
					zap.Int("vin", vin),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
				continue
			}

			for _, nftpoint := range objData.CreatePointOfNFTs {
				sat := satInputOffset + nftpoint.Offset
				if int(sat) > len(tx.NewNFTDataCreated) {
					break
				}
				tx.NewNFTDataCreated[sat].Invalid = true
			}

			satInputOffset += objData.Satoshi
			if int(satInputOffset) > len(tx.NewNFTDataCreated) {
				break
			}
		}

		// insert created NFT
		for createIdxInTx, nft := range tx.NewNFTDataCreated {
			if nft.Invalid { // nft removed
				continue
			}
			inFee := true
			satOutputOffset := uint64(0)
			for vout, output := range tx.TxOuts {
				if uint64(createIdxInTx) < satOutputOffset+output.Satoshi {
					createPoint := model.NFTCreatePoint{
						Height:     uint32(model.MEMPOOL_HEIGHT),
						IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
						Offset:     uint64(createIdxInTx) - satOutputOffset,
					}
					output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, &createPoint)

					newInscriptionInfo := &model.NewInscriptionInfo{
						NFTData:     nft,
						CreatePoint: createPoint,
						TxIdx:       uint64(startIdx + txIdx),
						TxId:        tx.TxId,
						IdxInTx:     uint32(createIdxInTx),
						InTxVout:    uint32(vout),
					}
					newInscriptions = append(newInscriptions, newInscriptionInfo)

					inFee = false
					break
				}
				satOutputOffset += output.Satoshi
			}

			// create nft may in fee
			if inFee {
				tx.NFTLostCnt += 1
				createPoint := model.NFTCreatePoint{
					Height:     uint32(model.MEMPOOL_HEIGHT),
					IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
					Offset:     uint64(createIdxInTx) - satOutputOffset, // global fee offset in coinbase
				}
				newInscriptionInfo := &model.NewInscriptionInfo{
					NFTData:     nft,
					CreatePoint: createPoint,
					TxIdx:       uint64(startIdx + txIdx),
					TxId:        tx.TxId,
					IdxInTx:     uint32(createIdxInTx),
					InTxVout:    tx.TxOutCnt,
				}
				newInscriptions = append(newInscriptions, newInscriptionInfo)
			}
		}
		nftIndexInBlock += uint64(len(tx.NewNFTDataCreated))

		// insert exsit NFT
		satInputOffset = uint64(0)
		for vin, input := range tx.TxIns {
			objData, ok := mpSpentUtxo[input.InputOutpointKey]
			if !ok {
				logger.Log.Info("tx-input-err",
					zap.String("txin", "input missing utxo"),
					zap.String("txid", tx.TxIdHex),
					zap.Int("vin", vin),

					zap.String("utxid", input.InputHashHex),
					zap.Uint32("vout", input.InputVout),
				)
				continue
			}

			for _, nftpoint := range objData.CreatePointOfNFTs {
				sat := satInputOffset + nftpoint.Offset
				inFee := true
				satOutputOffset := uint64(0)
				for _, output := range tx.TxOuts {
					if uint64(sat) < satOutputOffset+output.Satoshi {
						output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, &model.NFTCreatePoint{
							Height:     nftpoint.Height,
							IdxInBlock: nftpoint.IdxInBlock,
							Offset:     uint64(sat - satOutputOffset),
						})
						inFee = false
						break
					}
					satOutputOffset += output.Satoshi
				} // fixme: create nft may in fee

				// create nft may in fee
				if inFee {
					tx.NFTLostCnt += 1
				}
			}
			satInputOffset += objData.Satoshi
		}

		// store utxo nft point
		for vout, output := range tx.TxOuts {
			if output.LockingScriptUnspendable {
				continue
			}

			if objData, ok := mpSpentUtxo[output.OutpointKey]; ok {
				// not spent in self block
				objData.CreatePointOfNFTs = output.CreatePointOfNFTs
			} else if objData, ok := mpNewUtxo[output.OutpointKey]; ok {
				objData.CreatePointOfNFTs = output.CreatePointOfNFTs
			} else {
				logger.Log.Info("tx-output-restore-nft-err",
					zap.String("txout", "output missing utxo"),
					zap.String("txid", tx.TxIdHex),
					zap.Int("vout", vout),
				)
			}
		}
	}

	return
}

func UpdateNewNFTInRedis(pipe redis.Pipeliner, newInscriptions []*model.NewInscriptionInfo) {
	logger.Log.Info("UpdateNewNFTInRedis",
		zap.Int("new", len(newInscriptions)),
	)
	ctx := context.Background()

	for _, nftData := range newInscriptions {
		strInscriptionID := fmt.Sprintf("%si%d", utils.HashString(nftData.TxId), nftData.IdxInTx)

		// redis有序utxo数据成员
		member := &redis.Z{
			Score:  float64(nftData.CreatePoint.Height)*1000000000 + float64(nftData.CreatePoint.IdxInBlock),
			Member: strInscriptionID}
		pipe.ZAdd(ctx, "nfts", member) // 有序new nft数据添加
	}
}

// RemoveNewNFTInRedisStartFromBlockHeight 清理被重组区块内的新创建nft
func RemoveNewNFTInRedisStartFromBlockHeight(pipe redis.Pipeliner, height int) {
	logger.Log.Info("RemoveNewNFTInRedisAfterBlockHeight",
		zap.Int("height", height),
	)
	ctx := context.Background()
	strHeight := fmt.Sprintf("%d000000000", height)
	pipe.ZRemRangeByScore(ctx, "nfts", strHeight, "+inf") // 有序nft数据清理
}
