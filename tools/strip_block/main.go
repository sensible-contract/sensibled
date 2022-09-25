// go build -v sensibled/tools/strip_block
// ./strip_block -start 0

package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/parser"
	"sensibled/utils"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	startBlockHeight int
	endBlockHeight   int
	blocksPath       string
	blockMagicHex    string

	outputBlocksPath string
	gobFlushFrom     int
)

func init() {
	flag.StringVar(&blocksPath, "blocks", "", "blocks data path")
	flag.StringVar(&outputBlocksPath, "output", "blocks", "output blocks data path")

	flag.IntVar(&startBlockHeight, "start", 0, "start block height")
	flag.IntVar(&endBlockHeight, "end", 0, "end block height")

	flag.IntVar(&gobFlushFrom, "gob", -1, "gob flush block header cache after fileIdx")
	flag.Parse()

	blockMagicHex = "f9beb4d9"
}

func hasSensibleFlag(pkScript []byte) bool {
	return bytes.HasSuffix(pkScript, []byte("sensible")) || bytes.HasSuffix(pkScript, []byte("oraclesv"))
}

func main() {
	// GC
	go func() {
		for {
			runtime.GC()
			time.Sleep(time.Second * 1)
		}
	}()

	// 初始化区块
	bc, err := parser.NewBlockchain(false, blocksPath, blockMagicHex)
	if err != nil {
		logger.Log.Error("init chain error", zap.Error(err))
		return
	}

	// 重新扫区块头缓存
	if gobFlushFrom > 0 {
		bc.LastFileIdx = gobFlushFrom
	}

	// 初始化载入block header
	bc.InitLongestChainHeader()

	////////////////////////////////////////////////////////////////
	if endBlockHeight == 0 {
		endBlockHeight = len(bc.BlocksOfChainByHeight)
	}

	var wg sync.WaitGroup
	blocksDone := make(chan struct{}, 14)

	for nextBlockHeight := startBlockHeight; nextBlockHeight < endBlockHeight; nextBlockHeight++ {
		block, ok := bc.BlocksOfChainByHeight[nextBlockHeight]
		if !ok {
			// 若不是主链区块则退出
			break
		}

		bc.BlockData.SkipTo(block.FileIdx, block.FileOffset)
		// 获取所有Block字节
		rawblock, err := bc.BlockData.GetRawBlock()
		if err != nil {
			logger.Log.Error("get block error", zap.Error(err))
			break
		}
		if len(rawblock) < 80+9 { // block header + txn
			continue
		}

		blkId := block.Hash
		parser.InitBlock(block, rawblock)
		if !bytes.Equal(blkId, block.Hash) {
			logger.Log.Info("blkId not match hash(rawblk)",
				zap.Int("height", nextBlockHeight),
				zap.String("blkHash", block.HashHex),
				zap.Int("fileIdx", block.FileIdx),
				zap.Int("fileOffset", block.FileOffset))
			break
		}

		if nextBlockHeight%1000 == 0 {
			logger.Log.Info("dump",
				zap.Int("height", nextBlockHeight),
				zap.Int("fileIdx", block.FileIdx),
				zap.Int("fileOffset", block.FileOffset))
		}
		path := fmt.Sprintf("%s/%04d", outputBlocksPath, nextBlockHeight/1000)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			os.Mkdir(path, 0755)
		}

		blocksDone <- struct{}{}
		wg.Add(1)
		go func(block *model.Block, nextBlockHeight int) {
			defer wg.Done()

			block.Txs = parser.NewTxs(false, block.Raw[80:])

			stripRawBlock := make([]byte, len(block.Raw)+int(block.TxCnt)*36)

			copy(stripRawBlock[:80], block.Raw[:80])
			offset := 80
			offset += utils.EncodeVarIntForBlock(block.TxCnt, stripRawBlock[80:])
			for _, tx := range block.Txs {
				strip := true
				for _, output := range tx.TxOuts {
					if hasSensibleFlag(output.PkScript) {
						strip = false
						break
					}
				}

				if strip {
					txoffset := parser.NewRawTx(tx, stripRawBlock[offset:])
					offset += txoffset
				} else {
					copy(stripRawBlock[offset:], tx.Raw)
					offset += int(tx.Size)
				}
				copy(stripRawBlock[offset:], tx.TxId)
				offset += 32

				binary.LittleEndian.PutUint32(stripRawBlock[offset:offset+4], tx.Size)
				offset += 4

				tx.Raw = nil
			}

			if err := os.WriteFile(fmt.Sprintf("%s/%04d/%07d", outputBlocksPath, nextBlockHeight/1000, nextBlockHeight), stripRawBlock[:offset], 0644); err != nil {
				logger.Log.Info("write failed.")

			}
			block.Raw = nil
			block.Txs = nil
			<-blocksDone
		}(block, nextBlockHeight)
	}

	wg.Wait()

	logger.Log.Info("stoped")
}
