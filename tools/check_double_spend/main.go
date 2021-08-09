// go build -v tools/check_double_spend/main.go
// ./main -main 0000000000000000036b8e2c164cc37bc8460694c0cbe94ed4cc1de4dfcece35 -orphan 000000000000000008e031f99c8545487ac10fe8c9a09cdfbfef3688a53e7dba > tools/double-spend.out 2>&1

package main

import (
	"bytes"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"satoblock/logger"
	"satoblock/model"
	"satoblock/parser"
	"satoblock/utils"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	orphanBlockId  string
	mainBlockId    string
	endBlockHeight int
	blocksPath     string
	blockMagic     string
)

func init() {
	flag.StringVar(&orphanBlockId, "orphan", "", "orphan block id")
	flag.StringVar(&mainBlockId, "main", "", "main block id")
	flag.Parse()

	viper.SetConfigFile("conf/chain.yaml")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		} else {
			panic(fmt.Errorf("Fatal error config file: %s \n", err))
		}
	}

	blocksPath = viper.GetString("blocks")
	blockMagic = viper.GetString("magic")
}

func main() {
	// 初始化区块
	blockchain, err := parser.NewBlockchain(blocksPath, blockMagic)
	if err != nil {
		logger.Log.Error("init chain error", zap.Error(err))
		return
	}
	// 初始化载入block header
	blockchain.InitLongestChainHeader()

	////////////////////////////////////////////////////////////////
	commonHeight := 0
	blockIdHex := orphanBlockId
	orphanCount := 0

	orphanTxAllMap := make(map[string]*model.Tx, 0)
	orphanTxMap := make(map[string]*model.Tx, 0)
	orphanTxIdMap := make(map[string]string, 0)
	orphanSpentUtxoKeysMap := make(map[string]string, 0)
	for {
		block, ok := blockchain.Blocks[blockIdHex]
		if !ok {
			logger.Log.Error("orphan blockId not found",
				zap.String("blkId", blockIdHex))
			break
		}

		if _, ok := blockchain.BlocksOfChainById[blockIdHex]; ok {
			commonHeight = block.Height
			logger.Log.Info("finished orphan blocks",
				zap.Int("lastHeight", block.Height),
				zap.Int("nOrphan", orphanCount),
			)
			break
		}

		if ok := getBlockTxs(blockchain, block); !ok {
			logger.Log.Error("get orphan rawblock failed ",
				zap.String("blkId", blockIdHex))
			break
		}

		for idx, tx := range block.Txs {
			if idx == 0 {
				continue
			}
			orphanTxAllMap[tx.HashHex] = tx
			orphanTxIdMap[tx.HashHex] = fmt.Sprintf("%d: %d", block.Height, idx)
			for vin, input := range tx.TxIns {
				orphanSpentUtxoKeysMap[input.InputOutpointKey] = fmt.Sprintf("%s: %d", tx.HashHex, vin)
			}
		}

		blockIdHex = block.ParentHex
		orphanCount++
	}
	////////////////////////////////////////////////////////////////
	blockIdHex = mainBlockId
	mainCount := 0
	mainTxMap := make(map[string]*model.Tx, 0)
	for {
		block, ok := blockchain.BlocksOfChainById[blockIdHex]
		if !ok {
			logger.Log.Error("main blockId not found",
				zap.String("blkId", blockIdHex))
			break
		}

		if commonHeight == block.Height {
			logger.Log.Info("finished main blocks",
				zap.Int("lastHeight", block.Height),
				zap.Int("nMain", mainCount),
			)
			break
		}

		if ok := getBlockTxs(blockchain, block); !ok {
			logger.Log.Error("get main rawblock failed ",
				zap.String("blkId", blockIdHex))
			break
		}

		for idx, tx := range block.Txs {
			if idx == 0 {
				continue
			}

			if _, ok := orphanTxIdMap[tx.HashHex]; ok {
				continue
			}

			for vin, input := range tx.TxIns {
				if orphanTxLocation, ok := orphanSpentUtxoKeysMap[input.InputOutpointKey]; ok {
					// found utxo double spend
					orphanTxId := orphanTxLocation[:64]

					orphanTxMap[orphanTxId] = orphanTxAllMap[orphanTxId]
					mainTxMap[tx.HashHex] = tx

					orphanBlockLocation := orphanTxIdMap[orphanTxId]
					logger.Log.Info("found utxo double spend",
						zap.String("orphanBlockLocation", orphanBlockLocation),
						zap.String("orphanTxLocation", orphanTxLocation),
						zap.String("mainBlockLocation", fmt.Sprintf("%d: %d", block.Height, idx)),
						zap.String("mainTxLocation", fmt.Sprintf("%s: %d", tx.HashHex, vin)),
						zap.String("utxo-txid", input.InputHashHex),
						zap.Uint32("utxo-vout", input.InputVout),
					)
				}
			}
		}

		blockIdHex = block.ParentHex
		mainCount++
	}
	////////////////////////////////////////////////////////////////
	orphanAddressSatoshiMap := make(map[string]uint64, 0)
	orphanSpendOutpointCount := 0
	var orphanSpendOutAmount uint64
	for _, tx := range orphanTxMap {
		orphanSpendOutpointCount += int(tx.TxInCnt)
		for vout, output := range tx.TxOuts {
			// address
			scriptType := scriptDecoder.GetLockingScriptType(output.PkScript)
			txo := scriptDecoder.ExtractPkScriptForTxo(output.PkScript, scriptType)
			addr := utils.EncodeAddress(txo.AddressPkh, utils.PubKeyHashAddrIDMainNet)
			orphanAddressSatoshiMap[addr] += output.Satoshi
			orphanSpendOutAmount += output.Satoshi
			logger.Log.Info("orphanTx out",
				zap.Uint32("locktime", tx.LockTime),
				zap.String("txid", tx.HashHex),
				zap.Int("vout", vout),
				zap.String("address", addr),
				zap.Uint64("satoshi", output.Satoshi),
			)
		}
	}

	mainAddressSatoshiMap := make(map[string]uint64, 0)
	mainSpendOutpointCount := 0
	var mainSpendOutAmount uint64
	for _, tx := range mainTxMap {
		mainSpendOutpointCount += int(tx.TxInCnt)
		for vout, output := range tx.TxOuts {
			// address
			scriptType := scriptDecoder.GetLockingScriptType(output.PkScript)
			txo := scriptDecoder.ExtractPkScriptForTxo(output.PkScript, scriptType)
			addr := utils.EncodeAddress(txo.AddressPkh, utils.PubKeyHashAddrIDMainNet)
			mainAddressSatoshiMap[addr] += output.Satoshi
			mainSpendOutAmount += output.Satoshi
			logger.Log.Info("mainTx out",
				zap.String("txid", tx.HashHex),
				zap.Int("vout", vout),
				zap.String("address", addr),
				zap.Uint64("satoshi", output.Satoshi),
			)
		}
	}
	////////////////

	for addr, satoshi := range orphanAddressSatoshiMap {
		logger.Log.Info("orphan address amount",
			zap.String("address", addr),
			zap.Uint64("satoshi", satoshi),
		)
	}

	for addr, satoshi := range mainAddressSatoshiMap {
		logger.Log.Info("main address amount",
			zap.String("address", addr),
			zap.Uint64("satoshi", satoshi),
		)
	}
	////////////////
	logger.Log.Info("summray",
		zap.Int("nOrphanTx", len(orphanTxMap)),
		zap.Int("nOrphanTxInput", orphanSpendOutpointCount),
		zap.Uint64("orphanSpendOutAmount", orphanSpendOutAmount),

		zap.Int("nMainTx", len(mainTxMap)),
		zap.Int("nMainTxInput", mainSpendOutpointCount),
		zap.Uint64("mainSpendOutAmount", mainSpendOutAmount),
	)

	logger.Log.Info("stoped")
}

func getBlockTxs(bc *parser.Blockchain, block *model.Block) bool {
	bc.BlockData.SkipTo(block.FileIdx, block.FileOffset)
	// 获取所有Block字节
	rawblock, err := bc.BlockData.GetRawBlock()
	if err != nil {
		logger.Log.Error("get block error", zap.Error(err))
		return false
	}
	if len(rawblock) < 80+9 {
		return false
	}

	blkId := block.Hash
	parser.InitBlock(block, rawblock)
	if !bytes.Equal(blkId, block.Hash) {
		logger.Log.Info("blkId not match hash(rawblk)",
			zap.Int("height", block.Height),
			zap.String("blkId", utils.HashString(blkId)),
			zap.String("blkHash", block.HashHex),
			zap.Int("fileIdx", block.FileIdx),
			zap.Int("fileOffset", block.FileOffset))
		return false
	}

	block.Txs = parser.NewTxs(block.Raw[80:])

	return true
}
