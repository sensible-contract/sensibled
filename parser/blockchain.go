package parser

import (
	"bytes"
	"encoding/hex"
	"sensibled/loader"
	"sensibled/logger"
	"sensibled/model"
	"sensibled/task"
	utilsTask "sensibled/task/utils"
	"sensibled/utils"
	"sync"

	"go.uber.org/zap"
)

var NeedStop bool

type Blockchain struct {
	Blocks                map[string]*model.Block // 所有区块
	BlocksOfChainById     map[string]*model.Block // 按blkid主链区块
	BlocksOfChainByHeight map[int]*model.Block    // 按height主链区块
	MaxBlock              *model.Block
	GenesisBlock          *model.Block
	BlockData             *loader.BlockData
	LastFileIdx           int
	m                     sync.Mutex
}

func NewBlockchain(path string, magicHex string) (bc *Blockchain, err error) {
	magic, err := hex.DecodeString(magicHex)
	if err != nil {
		return nil, err
	}

	bc = new(Blockchain)
	bc.Blocks = make(map[string]*model.Block, 0)

	bc.LastFileIdx = loader.LoadFromGobFile("./cmd/headers-list.gob", bc.Blocks)

	bc.BlockData = loader.NewBlockData(path, magic)
	return
}

// ParseLongestChain 两遍遍历区块。先获取header，再遍历区块
func (bc *Blockchain) ParseLongestChain(startBlockHeight, endBlockHeight, batchTxCount int) int {
	blocksReady := make(chan *model.Block, 64)
	blocksDone := make(chan struct{}, 64)

	blocksStage := make(chan *model.Block, 64)

	// 并行解码区块，生产者
	go bc.InitLongestChainBlockByHeader(blocksDone, blocksReady, startBlockHeight, endBlockHeight, batchTxCount)

	// 按顺序消费解码后的区块
	go bc.ParseLongestChainBlockStart(blocksDone, blocksReady, blocksStage, startBlockHeight, endBlockHeight)

	// 并行消费处理后的区块
	lastHeight := bc.ParseLongestChainBlockEnd(blocksStage)
	return lastHeight
}

// InitLongestChainBlock 解码区块，生产者
func (bc *Blockchain) InitLongestChainBlockByHeader(blocksDone chan struct{}, blocksReady chan *model.Block, startBlockHeight, endBlockHeight, batchTxCount int) {
	var wg sync.WaitGroup

	if endBlockHeight < 0 {
		endBlockHeight = len(bc.BlocksOfChainByHeight)
	}

	var txCount int
	for nextBlockHeight := startBlockHeight; nextBlockHeight < endBlockHeight; nextBlockHeight++ {
		if NeedStop {
			break
		}

		block, ok := bc.BlocksOfChainByHeight[nextBlockHeight]
		if !ok {
			// 若不是主链区块则退出
			break
		}

		if batchTxCount > 0 && txCount > batchTxCount {
			break
		}
		txCount += int(block.TxCnt)

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
		InitBlock(block, rawblock)
		if !bytes.Equal(blkId, block.Hash) {
			logger.Log.Info("blkId not match hash(rawblk)",
				zap.Int("height", nextBlockHeight),
				zap.String("blkId", utils.HashString(blkId)),
				zap.String("blkHash", block.HashHex),
				zap.Int("fileIdx", block.FileIdx),
				zap.Int("fileOffset", block.FileOffset))
			break
		}

		blocksDone <- struct{}{}

		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()

			processBlock := &model.ProcessBlock{
				Height:           uint32(block.Height),
				NewUtxoDataMap:   make(map[string]*model.TxoData, block.TxCnt),
				SpentUtxoDataMap: make(map[string]*model.TxoData, block.TxCnt),
				SpentUtxoKeysMap: make(map[string]struct{}, block.TxCnt),
				TokenSummaryMap:  make(map[string]*model.TokenData, 1), // key: CodeHash+GenesisId  nft: CodeHash+GenesisId+tokenIdx
			}
			block.ParseData = processBlock
			block.Txs = NewTxs(block.Raw[80:])

			// 先并行分析区块。可执行一些区块内的独立预处理任务，不同区块会并行乱序执行
			task.ParseBlockParallel(block)

			block.Raw = nil
			blocksReady <- block
		}(block)
	}
	wg.Wait()

	close(blocksReady)
	logger.Log.Info("produce ok")
}

// ParseLongestChainBlock 按顺序消费解码后的区块
func (bc *Blockchain) ParseLongestChainBlockStart(blocksDone chan struct{}, blocksReady, blocksStage chan *model.Block, startBlockHeight, maxBlockHeight int) {
	blocksTotal := len(bc.BlocksOfChainById)

	withMempool := false
	if maxBlockHeight < 0 {
		withMempool = true
	}

	if maxBlockHeight < 0 || maxBlockHeight > blocksTotal {
		maxBlockHeight = blocksTotal
	}

	nextBlockHeight := startBlockHeight
	blockParseBufferBlock := make([]*model.Block, maxBlockHeight-startBlockHeight)
	for block := range blocksReady {
		// 暂存block
		if block.Height < maxBlockHeight {
			blockParseBufferBlock[block.Height-startBlockHeight] = block
		}

		// 按序
		if block.Height != nextBlockHeight {
			continue
		}
		for nextBlockHeight < maxBlockHeight {
			block = blockParseBufferBlock[nextBlockHeight-startBlockHeight]
			if block == nil { // 检查是否准备好
				break
			}

			<-blocksDone

			// 再串行分析区块。可执行一些严格要求按序处理的任务，区块会串行依次执行
			// 当串行执行到某个区块时，一定运行完毕了之前区块的所有任务和本区块的预处理任务
			task.ParseBlockSerialStart(withMempool, block)
			// block speed
			utilsTask.ParseBlockSpeed(len(block.Txs), len(model.GlobalNewUtxoDataMap), len(model.GlobalSpentUtxoDataMap),
				block.Height, maxBlockHeight)

			blocksStage <- block

			nextBlockHeight++
		}
		if nextBlockHeight >= maxBlockHeight {
			break
		}
	}
	close(blocksStage)
}

// ParseLongestChainBlock 再并行分析区块。接下来是无关顺序的收尾工作
func (bc *Blockchain) ParseLongestChainBlockEnd(blocksStage chan *model.Block) int {
	var wg sync.WaitGroup
	lastHeight := 0
	blocksLimit := make(chan struct{}, 64)
	for block := range blocksStage {
		lastHeight = block.Height
		blocksLimit <- struct{}{}
		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()
			task.ParseBlockParallelEnd(block)
			<-blocksLimit
		}(block)
	}
	wg.Wait()
	logger.Log.Info("consume ok")
	return lastHeight
}

// InitLongestChainHeader 初始化block header
func (bc *Blockchain) InitLongestChainHeader() bool {
	logger.Log.Info("load block header", zap.Int("last_file", bc.LastFileIdx))
	if err := bc.BlockData.SkipTo(bc.LastFileIdx, 0); err == nil {
		bc.LoadAllBlockHeaders()
	}

	if len(bc.Blocks) == 0 {
		logger.Log.Error("blocks not found, skip dump gob")
		return false
	}
	loader.DumpToGobFile("./cmd/headers-list.gob", bc.Blocks)

	bc.SetBlockHeight()
	bc.SelectLongestChain()
	return true
}

// LoadAllBlockHeaders 读取所有的rawBlock
func (bc *Blockchain) LoadAllBlockHeaders() {
	parsers := make(chan struct{}, 30)
	var wg sync.WaitGroup
	for idx := 0; ; idx++ {
		if NeedStop {
			break
		}
		// 获取所有Block Header字节，不要求有序返回或属于主链
		var rawblock []byte
		var err error
		rawblock, err = bc.BlockData.GetRawBlockHeader()
		if err != nil {
			logger.Log.Info("no more block header", zap.Error(err))
			break
		}
		if len(rawblock) < 80+9 { // block header + txn
			continue
		}

		parsers <- struct{}{}
		wg.Add(1)
		go func(rawblock []byte, fileidx, fileoffset int) {
			defer wg.Done()
			block := NewBlock(rawblock)
			block.FileIdx = fileidx
			block.FileOffset = fileoffset

			bc.m.Lock()

			if block.FileIdx > bc.LastFileIdx {
				bc.LastFileIdx = block.FileIdx
			}

			bc.Blocks[block.HashHex] = block
			bc.m.Unlock()

			<-parsers
		}(rawblock, bc.BlockData.CurrentId, bc.BlockData.LastOffset)

		// header speed
		utilsTask.ParseBlockSpeed(0, len(model.GlobalNewUtxoDataMap), len(model.GlobalSpentUtxoDataMap), idx, 0)
	}
	wg.Wait()
}

// SetBlockHeight 设置所有区块的高度，包括分支链的高度
func (bc *Blockchain) SetBlockHeight() {
	// logger.Log.Info("plain blocks count: %d", len(bc.Blocks))
	// 初始化
	for _, block := range bc.Blocks {
		block.Height = 0
	}
	for _, block := range bc.Blocks {
		// 已设置区块高度则跳过
		if block.Height > 0 {
			continue
		}
		// 未设置则检查parent block的高度
		thisBlock := block
		for {
			parentBlock, ok := bc.Blocks[thisBlock.ParentHex]
			// 如果找不到parent block，则初始化此block高度为1，后续处理时将统一减一
			if !ok {
				thisBlock.Height = 1
				break
			}
			// 如果能找到parent block，先串联block
			parentBlock.NextHex = thisBlock.HashHex
			// 若parent block 高度未设置，则继续看前面的Parent block
			if parentBlock.Height <= 0 {
				thisBlock = parentBlock
				continue
			}
			// 若parent block 高度已设置，则更新当前block高度
			thisBlock.Height = parentBlock.Height + 1
			break
		}
		// 依次更新后续block高度
		currBlock := thisBlock
		for {
			nextBlock, ok := bc.Blocks[currBlock.NextHex]
			if !ok {
				break
			}
			nextBlock.Height = currBlock.Height + 1
			currBlock = nextBlock
		}
		// 记录maxBlock
		if bc.MaxBlock == nil || currBlock.Height > bc.MaxBlock.Height {
			bc.MaxBlock = currBlock
		}
	}

	// 由于之前的高度是从1开始，现在统一减一
	for _, block := range bc.Blocks {
		block.Height -= 1
	}
}

// SelectLongestChain 提取最长主链
func (bc *Blockchain) SelectLongestChain() {
	bc.BlocksOfChainById = make(map[string]*model.Block, 0)
	bc.BlocksOfChainByHeight = make(map[int]*model.Block, 0)
	block := bc.MaxBlock
	logger.Log.Info("chain", zap.Int("maxheight", block.Height))
	for {
		bc.BlocksOfChainById[block.HashHex] = block
		bc.BlocksOfChainByHeight[block.Height] = block
		// 设置genesis
		bc.GenesisBlock = block
		var ok bool
		block, ok = bc.Blocks[block.ParentHex]
		if !ok {
			break
		}
	}
	logger.Log.Info("chain",
		zap.String("genesis", bc.GenesisBlock.HashHex),
		zap.Int("length", len(bc.BlocksOfChainById)),
		zap.Int("allBlks", len(bc.Blocks)),
	)
}

// GetBlockSyncCommonBlockHeight 获取区块同步起始的共同区块高度
func (bc *Blockchain) GetBlockSyncCommonBlockHeight(endBlockHeight int) (heigth, orphanCount, newblock int) {
	lastBlock, err := loader.GetLatestBlockFromDB()
	if err != nil {
		panic("sync check by GetLatestBlocksFromDB, but failed.")
	}
	blockIdHex := utils.HashString(lastBlock.BlockId)

	if endBlockHeight < 0 || endBlockHeight > len(bc.BlocksOfChainById) {
		endBlockHeight = len(bc.BlocksOfChainById)
	}

	orphanCount = 0
	for {
		block, ok := bc.Blocks[blockIdHex]
		if !ok {
			logger.Log.Error("blockId not found", zap.String("blkId", blockIdHex))
			break
		}

		if _, ok := bc.BlocksOfChainById[blockIdHex]; ok {
			newblock = endBlockHeight - int(block.Height) - 1
			logger.Log.Info("shoud sync block",
				zap.Int("lastHeight", block.Height),
				zap.Int("nOrphan", orphanCount),
				zap.Int("nBlkNew", newblock))
			return block.Height, orphanCount, newblock
		}

		orphanCount++
		logger.Log.Info("orphan block happen",
			zap.String("blkId", blockIdHex),
			zap.Int("orphan", orphanCount),
		)
		blockIdHex = block.ParentHex
	}
	panic("sync check, but found more then 1000 orphan blocks.")
}
