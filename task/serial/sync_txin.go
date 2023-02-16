package serial

import (
	"sensibled/logger"
	"sensibled/model"
	scriptDecoder "sensibled/parser/script"
	"sensibled/store"
	"sensibled/utils"

	"go.uber.org/zap"
)

// ParseBlockTxNFTsInAndOutSerial all tx input/output info
func ParseBlockTxNFTsInAndOutSerial(block *model.Block) {
	var coinbaseCreatePointOfNFTs []*model.NFTCreatePoint
	satFeeOffset := utils.CalcBlockSubsidy(block.Height)
	nftIndexInBlock := uint64(0)
	for _, tx := range block.Txs[1:] {
		// count tx fee
		satInputAmount := uint64(0)
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
			satInputAmount += objData.Satoshi
		}
		satOutputAmount := uint64(0)
		for _, output := range tx.TxOuts {
			satOutputAmount += output.Satoshi
		}
		satFeeAmount := satInputAmount - satOutputAmount

		// invalid exist nft recreate
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
				if int(sat) > len(tx.CreateNFTData) {
					break
				}
				tx.CreateNFTData[sat].Invalid = true
			}

			satInputOffset += objData.Satoshi
			if int(satInputOffset) > len(tx.CreateNFTData) {
				break
			}
		}

		// insert created NFT
		for createIdxInTx, nft := range tx.CreateNFTData {
			if nft.Invalid { // nft removed
				continue
			}
			inFee := true
			satOutputOffset := uint64(0)
			for _, output := range tx.TxOuts {
				if uint64(createIdxInTx) < satOutputOffset+output.Satoshi {
					createPoint := model.NFTCreatePoint{
						Height:     uint32(block.Height),
						IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
						Offset:     uint64(createIdxInTx) - satOutputOffset,
					}
					output.CreatePointOfNFTs = append(output.CreatePointOfNFTs, &createPoint)

					// global store new nft
					model.GlobalNewInscriptions = append(model.GlobalNewInscriptions, &model.InscriptionID{
						CreatePoint: createPoint,
						TxId:        tx.TxId,
						IdxInTx:     uint64(createIdxInTx),
					})

					inFee = false
					break
				}
				satOutputOffset += output.Satoshi
			}

			// create nft may in fee
			if inFee {
				tx.NFTLostCnt += 1
				createPoint := model.NFTCreatePoint{
					Height:     uint32(block.Height),
					IdxInBlock: nftIndexInBlock + uint64(createIdxInTx),
					Offset:     uint64(createIdxInTx) - satOutputOffset + satFeeOffset, // global fee offset in coinbase
				}
				coinbaseCreatePointOfNFTs = append(coinbaseCreatePointOfNFTs, &createPoint)

				// global store new nft
				model.GlobalNewInscriptions = append(model.GlobalNewInscriptions, &model.InscriptionID{
					CreatePoint: createPoint,
					TxId:        tx.TxId,
					IdxInTx:     uint64(createIdxInTx),
				})
			}
		}
		nftIndexInBlock += uint64(len(tx.CreateNFTData))

		// insert exsit NFT
		satInputOffset = uint64(0)
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
					coinbaseCreatePointOfNFTs = append(coinbaseCreatePointOfNFTs, &model.NFTCreatePoint{
						Height:     nftpoint.Height,
						IdxInBlock: nftpoint.IdxInBlock,
						Offset:     uint64(sat) - satOutputOffset + satFeeOffset, // global fee offset in coinbase
					})
				}
			}
			satInputOffset += objData.Satoshi
		}
		satFeeOffset += satFeeAmount

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

// SyncBlockTxInputDetail all tx input info
func SyncBlockTxInputDetail(block *model.Block) {
	var commonObjData *model.TxoData = &model.TxoData{
		Satoshi:     utils.CalcBlockSubsidy(block.Height),
		AddressData: &scriptDecoder.AddressData{},
	}

	for txIdx, tx := range block.Txs {
		isCoinbase := (txIdx == 0)

		for vin, input := range tx.TxIns {
			objData := commonObjData
			if !isCoinbase {
				objData.Satoshi = 0
				if obj, ok := block.ParseData.SpentUtxoDataMap[input.InputOutpointKey]; ok {
					objData = obj
				} else {
					logger.Log.Info("tx-input-err",
						zap.String("txin", "input missing utxo"),
						zap.String("txid", tx.TxIdHex),
						zap.Int("vin", vin),

						zap.String("utxid", input.InputHashHex),
						zap.Uint32("vout", input.InputVout),
					)
				}
			}
			tx.InputsValue += objData.Satoshi
			tx.NFTInputsCnt += uint64(len(input.CreatePointOfNFTs))

			address := ""
			if objData.AddressData.HasAddress {
				address = string(objData.AddressData.AddressPkh[:]) // 20 bytes
			}

			// address tx历史记录
			if objData.AddressData.HasAddress {
				block.ParseData.AddrPkhInTxMap[address] = append(block.ParseData.AddrPkhInTxMap[address], txIdx)
			}

			// set sensible flag
			if objData.AddressData.CodeType != scriptDecoder.CodeType_NONE {
				tx.IsSensible = true
			}

			// 解锁脚本一般可安全清理
			scriptsig := ""
			if !isScriptSigPrune {
				scriptsig = string(input.ScriptSig)
			}
			scriptwits := ""
			if !isScriptSigPrune {
				scriptwits = string(input.ScriptWitness)
			}

			// 清理非sensible且无地址的锁定脚本
			pkscript := ""
			if !isPkScriptPrune || tx.IsSensible || objData.AddressData.HasAddress {
				pkscript = string(objData.PkScript)
			}

			nftPointsBuf := make([]byte, len(input.CreatePointOfNFTs)*3*8)
			model.DumpNFTCreatePoints(nftPointsBuf, input.CreatePointOfNFTs)

			if _, err := store.SyncStmtTxIn.Exec(
				uint32(block.Height),
				uint64(txIdx),
				string(tx.TxId),
				uint32(vin),
				scriptsig, // prune string(input.ScriptSig),
				scriptwits,
				uint32(input.Sequence),

				uint64(len(input.CreatePointOfNewNFTs)), // new nft count

				uint32(objData.BlockHeight),
				uint64(objData.TxIdx),
				string(input.InputHash),
				input.InputVout,
				address,
				uint32(objData.AddressData.CodeType),
				objData.Satoshi,
				string(objData.ScriptType),
				pkscript,

				// nft
				uint64(len(input.CreatePointOfNFTs)),
				string(nftPointsBuf),
			); err != nil {
				logger.Log.Info("sync-txin-full-err",
					zap.String("sync", "txin full err"),
					zap.String("txid", tx.TxIdHex),
					zap.Uint32("vin", uint32(vin)),
					zap.String("err", err.Error()),
				)
			}
		}
	}
}
