package serial

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"
	"unisatd/utils"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func getTxFee(tx *model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData) (satInputAmount, satOutputAmount uint64) {
	for vin, input := range tx.TxIns {
		var objData *model.TxoData
		if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
			objData = obj
		} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
			objData = obj
		} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
			objData = obj
		} else {
			logger.Log.Info("tx-fee-input-err",
				zap.String("txin", "input missing utxo"),
				zap.String("txid", tx.TxIdHex),
				zap.Int("vin", vin),

				zap.String("utxid", input.InputHashHex),
				zap.Uint32("vout", input.InputVout),
			)
			continue
		}
		satInputAmount += objData.Satoshi
	}
	for _, output := range tx.TxOuts {
		satOutputAmount += output.Satoshi
	}

	return satInputAmount, satOutputAmount
}

// ParseMempoolBatchTxNFTsInAndOutSerial all tx input/output info
func ParseMempoolBatchTxNFTsInAndOutSerial(startIdx int, nftIndexInBlock, nftStartNumber uint64,
	txs []*model.Tx, mpNewUtxo, removeUtxo, mpSpentUtxo map[string]*model.TxoData) (nftIndexInBlockAfter uint64, newInscriptions, newBRC20Inscriptions []*model.NewInscriptionInfo) {

	for txIdx, tx := range txs {
		satInputAmount, satOutputAmount := getTxFee(tx, mpNewUtxo, removeUtxo, mpSpentUtxo)
		// invalid exist nft recreate
		satInputOffset := uint64(0)
		for vin, input := range tx.TxIns {

			var objData *model.TxoData
			if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				logger.Log.Info("tx-new-nft-input-err",
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
				if int(sat) >= len(tx.NewNFTDataCreated) {
					continue
				}
				tx.NewNFTDataCreated[sat].Invalid = true
			}

			satInputOffset += objData.Satoshi
			if int(satInputOffset) >= len(tx.NewNFTDataCreated) {
				break
			}
		}

		// insert created NFT
		for createIdxInTx, nft := range tx.NewNFTDataCreated {
			if nft.Invalid { // nft removed
				continue
			}
			createPoint := &model.NFTCreatePoint{
				Height:     uint32(model.MEMPOOL_HEIGHT),
				IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
				HasMoved:   false,
				IsBRC20:    nft.IsBRC20,
			}
			newInscriptionInfo := &model.NewInscriptionInfo{
				NFTData:     nft,
				CreatePoint: createPoint,
				TxIdx:       uint64(startIdx + txIdx),
				TxId:        tx.TxId,
				IdxInTx:     uint32(createIdxInTx),

				InputsValue:  satInputAmount,
				OutputsValue: satOutputAmount,
				Ordinal:      0, // fixme: missing ordinal, todo
				Number:       nftStartNumber,
				BlockTime:    0,
			}
			nftStartNumber += 1
			inFee := true
			satOutputOffset := uint64(0)
			for vout, output := range tx.TxOuts {
				if uint64(createIdxInTx) < satOutputOffset+output.Satoshi {
					createPoint.Offset = uint64(createIdxInTx) - satOutputOffset
					newInscriptionInfo.InTxVout = uint32(vout)
					newInscriptionInfo.Satoshi = output.Satoshi
					newInscriptionInfo.PkScript = output.PkScript
					output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, createPoint)
					inFee = false
					if nft.IsBRC20 {
						newBRC20Inscriptions = append(newBRC20Inscriptions, newInscriptionInfo)
					}
					break
				}
				satOutputOffset += output.Satoshi
			}
			// create nft may in fee
			if inFee {
				tx.NFTLostCnt += 1
				createPoint.Offset = uint64(createIdxInTx) - satOutputOffset
				newInscriptionInfo.InTxVout = tx.TxOutCnt
				createPoint.HasMoved = true
			}
			newInscriptions = append(newInscriptions, newInscriptionInfo)
		}
		nftIndexInBlock += uint64(len(tx.NewNFTDataCreated))

		// insert exsit NFT
		satInputOffset = uint64(0)
		for vin, input := range tx.TxIns {
			var objData *model.TxoData
			if obj, ok := mpNewUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := removeUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else if obj, ok := mpSpentUtxo[input.InputOutpointKey]; ok {
				objData = obj
			} else {
				logger.Log.Info("tx-exist-nft-input-err",
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
				for vout, output := range tx.TxOuts {
					if uint64(sat) < satOutputOffset+output.Satoshi {
						movetoCreatePoint := &model.NFTCreatePoint{
							Height:     nftpoint.Height,
							IdxInBlock: nftpoint.IdxInBlock,
							Offset:     uint64(sat - satOutputOffset),
							HasMoved:   true,
							IsBRC20:    nftpoint.IsBRC20,
						}
						output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, movetoCreatePoint)
						inFee = false

						// record brc20 first transfer
						if !nftpoint.HasMoved && nftpoint.IsBRC20 {
							newInscriptionInfo := &model.NewInscriptionInfo{
								NFTData:     &scriptDecoder.NFTData{},
								CreatePoint: movetoCreatePoint,

								Height: uint32(model.MEMPOOL_HEIGHT),
								TxIdx:  uint64(startIdx + txIdx),
								TxId:   tx.TxId,

								InputsValue:  satInputAmount,
								OutputsValue: satOutputAmount,
								Ordinal:      0, // fixme: missing ordinal, todo

								InTxVout:  uint32(vout),
								Satoshi:   output.Satoshi,
								PkScript:  output.PkScript,
								BlockTime: 0,
							}
							newBRC20Inscriptions = append(newBRC20Inscriptions, newInscriptionInfo)
						}
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
			if objData, ok := mpNewUtxo[output.OutpointKey]; ok {
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

	return nftIndexInBlock, newInscriptions, newBRC20Inscriptions
}

func UpdateNewNFTDataInPika(newInscriptions []*model.NewInscriptionInfo) bool {
	logger.Log.Info("UpdateNewNFTDataInPika",
		zap.Int("new", len(newInscriptions)),
	)
	ctx := context.Background()

	type Item struct {
		CreateIdx string
		Data      string
	}
	items := make([]*Item, 0)
	for _, nftData := range newInscriptions {
		items = append(items, &Item{
			CreateIdx: nftData.CreatePoint.GetCreateIdxKey(),
			Data:      nftData.DumpString(),
		})
	}

	maxSize := 1000000
	for idx := 0; idx < len(items); {
		pikaPipe := rdb.RdbAddrTxClient.Pipeline()
		size := 0
		for ; size < maxSize && idx < len(items); idx++ {
			// 有序address tx history数据添加
			pikaPipe.Set(ctx, "nb"+items[idx].CreateIdx, items[idx].Data, 0)
			size += 32 + len(items[idx].Data)
		}
		if _, err := pikaPipe.Exec(ctx); err != nil && err != redis.Nil {
			logger.Log.Error("pika address exec failed", zap.Error(err))
			model.NeedStop = true
			return false
		}
	}
	return true
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
			Score:  float64(nftData.CreatePoint.Height)*model.HEIGHT_MUTIPLY + float64(nftData.CreatePoint.IdxInBlock),
			Member: strInscriptionID}
		pipe.ZAdd(ctx, "nfts", member) // 有序new nft数据添加
	}
}

func UpdateNewNFTBodyInCache(newInscriptions []*model.NewInscriptionInfo) {
	logger.Log.Info("UpdateNewNFTBodyInCache",
		zap.Int("new", len(newInscriptions)),
	)
	ctx := context.Background()

	for _, nftData := range newInscriptions {
		strInscriptionID := fmt.Sprintf("nft:%si%d", utils.HashString(nftData.TxId), nftData.IdxInTx)

		var data [2]byte
		binary.LittleEndian.PutUint16(data[:], uint16(len(nftData.NFTData.ContentType)))
		rdb.CacheClient.Set(ctx, strInscriptionID,
			string(data[:])+
				string(nftData.NFTData.ContentType)+
				string(nftData.NFTData.ContentBody),
			7*24*time.Hour) // 有序new nft数据添加
	}
}

// RemoveNewNFTInRedisStartFromBlockHeight 清理被重组区块内的新创建nft
func RemoveNewNFTInRedisStartFromBlockHeight(pipe redis.Pipeliner, height int) {
	logger.Log.Info("RemoveNewNFTInRedisAfterBlockHeight",
		zap.Int("height", height),
	)
	ctx := context.Background()
	strHeight := fmt.Sprintf("%d", height*model.HEIGHT_MUTIPLY)
	pipe.ZRemRangeByScore(ctx, "nfts", strHeight, "+inf") // 有序nft数据清理
}
