package serial

import (
	"context"
	"fmt"
	"unisatd/logger"
	"unisatd/model"
	scriptDecoder "unisatd/parser/script"
	"unisatd/rdb"
	"unisatd/utils"

	redis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func getTxFee(tx *model.Tx, spentUtxoDataMap map[string]*model.TxoData) (satInputAmount, satOutputAmount uint64) {
	for vin, input := range tx.TxIns {
		objData, ok := spentUtxoDataMap[input.InputOutpointKey]
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
		satInputAmount += objData.Satoshi
	}
	for _, output := range tx.TxOuts {
		satOutputAmount += output.Satoshi
	}

	return satInputAmount, satOutputAmount
}

func invalidTxRecreateNFT(tx *model.Tx, spentUtxoDataMap map[string]*model.TxoData) {
	// invalid exist nft recreate
	satInputOffset := uint64(0)
	for vin, input := range tx.TxIns {
		objData, ok := spentUtxoDataMap[input.InputOutpointKey]
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
}

// ParseBlockTxNFTsInAndOutSerial all tx input/output info
func ParseBlockTxNFTsInAndOutSerial(nftStartNumber uint64, block *model.Block) {
	var coinbaseCreatePointOfNFTs []*model.NFTCreatePoint
	satFeeOffset := utils.CalcBlockSubsidy(block.Height)
	nftIndexInBlock := uint64(0)
	// 跳过coinbase
	for txIdx, tx := range block.Txs[1:] {

		invalidTxRecreateNFT(tx, block.ParseData.SpentUtxoDataMap)

		satInputAmount, satOutputAmount := getTxFee(tx, block.ParseData.SpentUtxoDataMap)
		// insert created NFT
		for createIdxInTx, nft := range tx.NewNFTDataCreated {
			if nft.Invalid { // nft removed
				continue
			}
			createPoint := &model.NFTCreatePoint{
				Height:     uint32(block.Height),
				IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
				HasMoved:   false,
				IsBRC20:    nft.IsBRC20,
			}
			newInscriptionInfo := &model.NewInscriptionInfo{
				NFTData:     nft,
				CreatePoint: createPoint,
				TxIdx:       uint64(txIdx + 1),
				TxId:        tx.TxId,
				IdxInTx:     uint32(createIdxInTx),

				InputsValue:  satInputAmount,
				OutputsValue: satOutputAmount,
				Ordinal:      0, // fixme: missing ordinal, todo
				Number:       nftStartNumber,
				BlockTime:    block.BlockTime,
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
						block.ParseData.NewBRC20Inscriptions = append(block.ParseData.NewBRC20Inscriptions, newInscriptionInfo)
					}
					break
				}
				satOutputOffset += output.Satoshi
			}
			// create nft may in fee
			if inFee {
				tx.NFTLostCnt += 1
				createPoint.Offset = uint64(createIdxInTx) - satOutputOffset + satFeeOffset // global fee offset in coinbase
				newInscriptionInfo.InTxVout = tx.TxOutCnt
				createPoint.HasMoved = true
				coinbaseCreatePointOfNFTs = append(coinbaseCreatePointOfNFTs, createPoint)
			}
			block.ParseData.NewInscriptions = append(block.ParseData.NewInscriptions, newInscriptionInfo)
			model.GlobalNewInscriptions = append(model.GlobalNewInscriptions, newInscriptionInfo)
		}
		nftIndexInBlock += uint64(len(tx.NewNFTDataCreated))

		// insert exist NFT
		satInputOffset := uint64(0)
		for vin, input := range tx.TxIns {
			objData, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]
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

						// record brc20 first transfer
						if !nftpoint.HasMoved && nftpoint.IsBRC20 {
							newInscriptionInfo := &model.NewInscriptionInfo{
								NFTData:     &scriptDecoder.NFTData{},
								CreatePoint: movetoCreatePoint,

								Height: uint32(block.Height),
								TxIdx:  uint64(txIdx + 1),
								TxId:   tx.TxId,

								InputsValue:  satInputAmount,
								OutputsValue: satOutputAmount,
								Ordinal:      0, // fixme: missing ordinal, todo

								InTxVout:  uint32(vout),
								Satoshi:   output.Satoshi,
								PkScript:  output.PkScript,
								BlockTime: block.BlockTime,
							}
							block.ParseData.NewBRC20Inscriptions = append(block.ParseData.NewBRC20Inscriptions, newInscriptionInfo)
						}

						inFee = false
						break
					}
					satOutputOffset += output.Satoshi
				} // fixme: create nft may in fee

				// create nft may in fee
				if inFee {
					tx.NFTLostCnt += 1
					coinbaseCreatePointOfNFTs = append(coinbaseCreatePointOfNFTs, &model.NFTCreatePoint{
						Height:     nftpoint.Height,
						IdxInBlock: nftpoint.IdxInBlock,
						Offset:     uint64(sat) - satOutputOffset + satFeeOffset, // global fee offset in coinbase
						HasMoved:   true,
						IsBRC20:    nftpoint.IsBRC20,
					})
				}
			}
			satInputOffset += objData.Satoshi
		}

		satFeeOffset += satInputAmount - satOutputAmount

		// store utxo nft point
		for vout, output := range tx.TxOuts {
			if output.LockingScriptUnspendable {
				continue
			}

			if objData, ok := block.ParseData.SpentUtxoDataMap[output.OutpointKey]; ok {
				// not spent in self block
				objData.CreatePointOfNFTs = output.CreatePointOfNFTs
			} else if objData, ok := block.ParseData.NewUtxoDataMap[output.OutpointKey]; ok {
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

	// coinbase
	coinbaseTx := block.Txs[0]

	// update coinbase input nft
	coinbaseTx.TxIns[0].CreatePointOfNFTs = coinbaseCreatePointOfNFTs
	for _, nftpoint := range coinbaseCreatePointOfNFTs {
		inFee := true
		sat := nftpoint.Offset
		satOutputOffset := uint64(0)
		for _, output := range coinbaseTx.TxOuts {
			if uint64(sat) < satOutputOffset+output.Satoshi {
				output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, &model.NFTCreatePoint{
					Height:     nftpoint.Height,
					IdxInBlock: nftpoint.IdxInBlock,
					Offset:     uint64(sat - satOutputOffset),
					HasMoved:   true,
					IsBRC20:    nftpoint.IsBRC20,
				})
				inFee = false
				break
			}
			satOutputOffset += output.Satoshi
		}
		if inFee {
			coinbaseTx.NFTLostCnt += 1
		}
	}

	// store utxo nft point
	for vout, output := range coinbaseTx.TxOuts {
		if output.LockingScriptUnspendable {
			continue
		}

		if objData, ok := block.ParseData.NewUtxoDataMap[output.OutpointKey]; ok {
			objData.CreatePointOfNFTs = output.CreatePointOfNFTs
		} else {
			logger.Log.Info("coinbase-output-restore-nft-err",
				zap.String("txout", "output is not utxo"),
				zap.String("txid", coinbaseTx.TxIdHex),
				zap.Int("vout", vout),
			)
		}
	}
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

// RemoveNewNFTInRedisStartFromBlockHeight 清理被重组区块内的新创建nft
func RemoveNewNFTInRedisStartFromBlockHeight(pipe redis.Pipeliner, height int) {
	logger.Log.Info("RemoveNewNFTInRedisAfterBlockHeight",
		zap.Int("height", height),
	)
	ctx := context.Background()
	strHeight := fmt.Sprintf("%d", height*model.HEIGHT_MUTIPLY)
	pipe.ZRemRangeByScore(ctx, "nfts", strHeight, "+inf") // 有序nft数据清理
}

// CountNewNFTInRedisBeforeBlockHeight 清理被重组区块内的新创建nft
func CountNewNFTInRedisBeforeBlockHeight(height int) (nftNumber uint64) {
	logger.Log.Info("CountNewNFTInRedisBeforeBlockHeight",
		zap.Int("height", height),
	)
	ctx := context.Background()
	strHeight := fmt.Sprintf("%d", height*model.HEIGHT_MUTIPLY)
	n, err := rdb.RdbBalanceClient.ZCount(ctx, "nfts", "-inf", strHeight).Result()
	if err != nil {
		logger.Log.Info("get nftNumber from redis failed", zap.Error(err))
		return 0
	}

	return uint64(n)
}
