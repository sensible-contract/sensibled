package parser

import (
	"bytes"
	"encoding/hex"
	"log"
	"satoblock/loader"
	"satoblock/model"
	"satoblock/task"
	serialTask "satoblock/task/serial"
	utilsTask "satoblock/task/utils"
	"satoblock/utils"
	"sync"
)

type Blockchain struct {
	Blocks                map[string]*model.Block // 所有区块
	BlocksOfChainById     map[string]*model.Block // 按blkid主链区块
	BlocksOfChainByHeight map[int]*model.Block    // 按height主链区块
	ParsedBlocks          map[string]bool         // 主链已分析区块
	MaxBlock              *model.Block
	GenesisBlock          *model.Block
	BlockData             *loader.BlockData
	m                     sync.Mutex
}

func NewBlockchain(path string, magicHex string) (bc *Blockchain, err error) {
	magic, err := hex.DecodeString(magicHex)
	if err != nil {
		return nil, err
	}

	bc = new(Blockchain)
	bc.Blocks = make(map[string]*model.Block, 0)
	bc.ParsedBlocks = make(map[string]bool, 0)

	loader.LoadFromGobFile("./headers-list.gob", bc.Blocks)

	bc.BlockData, err = loader.NewBlockData(path, magic)
	if err != nil {
		return nil, err
	}
	return
}

// ParseLongestChain 两遍遍历区块。先获取header，再遍历区块
func (bc *Blockchain) ParseLongestChain(startBlockHeight, endBlockHeight int, isFull bool) {
	blocksReady := make(chan *model.Block, 64)
	blocksDone := make(chan struct{}, 64)

	blocksStage := make(chan *model.Block, 64)

	// 并行解码区块，生产者
	go bc.InitLongestChainBlockByHeader(blocksDone, blocksReady, startBlockHeight, endBlockHeight)

	// 按顺序消费解码后的区块
	go bc.ParseLongestChainBlockStart(blocksDone, blocksReady, blocksStage, startBlockHeight, endBlockHeight)

	// 并行消费处理后的区块
	bc.ParseLongestChainBlockEnd(blocksStage)

	// 最后分析执行
	task.ParseEnd(isFull)
}

// InitLongestChainBlock 解码区块，生产者
func (bc *Blockchain) InitLongestChainBlockByHeader(blocksDone chan struct{}, blocksReady chan *model.Block, startBlockHeight, endBlockHeight int) {
	var wg sync.WaitGroup

	if endBlockHeight < 0 {
		endBlockHeight = len(bc.BlocksOfChainByHeight)
	}
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
			log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}

		blkId := block.Hash
		InitBlock(block, rawblock)
		if !bytes.Equal(blkId, block.Hash) {
			log.Printf("block id not match")
			break
		}

		blocksDone <- struct{}{}

		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()

			txs := NewTxs(block.Raw[80:])

			block.TxCnt = len(txs)
			block.Txs = txs

			processBlock := &model.ProcessBlock{
				Height:           uint32(block.Height),
				NewUtxoDataMap:   make(map[string]*model.TxoData, 1),
				SpentUtxoDataMap: make(map[string]*model.TxoData, 1),
				SpentUtxoKeysMap: make(map[string]bool, 1),
				TokenSummaryMap:  make(map[string]*model.TokenData, 1), // key: CodeHash+GenesisId  nft: CodeHash+GenesisId+tokenIdx
			}
			block.ParseData = processBlock

			// 先并行分析区块。可执行一些区块内的独立预处理任务，不同区块会并行乱序执行
			task.ParseBlockParallel(block)

			block.Raw = nil
			blocksReady <- block
		}(block)
	}
	wg.Wait()

	close(blocksReady)
	log.Printf("produce ok")
}

// ParseLongestChainBlock 按顺序消费解码后的区块
func (bc *Blockchain) ParseLongestChainBlockStart(blocksDone chan struct{}, blocksReady, blocksStage chan *model.Block, startBlockHeight, maxBlockHeight int) {
	blocksTotal := len(bc.BlocksOfChainById)
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
			task.ParseBlockSerialStart(block)
			// block speed
			utilsTask.ParseBlockSpeed(len(block.Txs), len(serialTask.GlobalNewUtxoDataMap), len(serialTask.GlobalSpentUtxoDataMap),
				block.Height, maxBlockHeight)

			blocksStage <- block

			nextBlockHeight++
		}
		if nextBlockHeight >= maxBlockHeight {
			break
		}
	}
	close(blocksStage)
	log.Printf("stage ok")
}

// ParseLongestChainBlock 再并行分析区块。接下来是无关顺序的收尾工作
func (bc *Blockchain) ParseLongestChainBlockEnd(blocksStage chan *model.Block) {
	var wg sync.WaitGroup
	blocksLimit := make(chan struct{}, 64)
	for block := range blocksStage {
		blocksLimit <- struct{}{}
		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()
			task.ParseBlockParallelEnd(block)
			<-blocksLimit
		}(block)
	}
	wg.Wait()
	log.Printf("consume ok")
}

// InitLongestChainHeader 初始化block header
func (bc *Blockchain) InitLongestChainHeader() {
	maxFileIdx := 0
	maxFileOffset := 0
	for _, blk := range bc.Blocks {
		if blk.FileIdx > maxFileIdx {
			maxFileIdx = blk.FileIdx
			maxFileOffset = blk.FileOffset
		} else if blk.FileIdx == maxFileIdx {
			if blk.FileOffset > maxFileOffset {
				maxFileOffset = blk.FileOffset
			}
		}
	}

	lastBlockHeadersCount := len(bc.Blocks)
	if err := bc.BlockData.SkipTo(maxFileIdx, 0); err == nil {
		bc.LoadAllBlockHeaders()
	}

	if len(bc.Blocks) > lastBlockHeadersCount {
		loader.DumpToGobFile("./headers-list.gob", bc.Blocks)
	}

	bc.SetBlockHeight()
	bc.SelectLongestChain()
}

// LoadAllBlockHeaders 读取所有的rawBlock
func (bc *Blockchain) LoadAllBlockHeaders() {
	parsers := make(chan struct{}, 30)
	var wg sync.WaitGroup
	for idx := 0; ; idx++ {
		// 获取所有Block Header字节，不要求有序返回或属于主链
		var rawblock []byte
		var err error
		rawblock, err = bc.BlockData.GetRawBlockHeader()

		if err != nil {
			// log.Printf("no more block header: %v", err)
			break
		}
		if len(rawblock) < 80 {
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
			if _, ok := bc.Blocks[block.HashHex]; !ok {
				bc.Blocks[block.HashHex] = block
			}
			bc.m.Unlock()

			<-parsers
		}(rawblock, bc.BlockData.CurrentId, bc.BlockData.LastOffset)

		// header speed
		utilsTask.ParseBlockSpeed(0, len(serialTask.GlobalNewUtxoDataMap), len(serialTask.GlobalSpentUtxoDataMap), idx, 0)
	}
	wg.Wait()
}

// SetBlockHeight 设置所有区块的高度，包括分支链的高度
func (bc *Blockchain) SetBlockHeight() {
	log.Printf("plain blocks count: %d", len(bc.Blocks))
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
}

// SelectLongestChain 提取最长主链
func (bc *Blockchain) SelectLongestChain() {
	bc.BlocksOfChainById = make(map[string]*model.Block, 0)
	bc.BlocksOfChainByHeight = make(map[int]*model.Block, 0)
	block := bc.MaxBlock
	for {
		// 由于之前的高度是从1开始，现在统一减一
		block.Height -= 1
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
	log.Printf("genesis block: %s", bc.GenesisBlock.HashHex)
	log.Printf("chain blocks count: %d", len(bc.BlocksOfChainById))
}

// GetBlockSyncCommonBlockHeight 获取区块同步起始的共同区块高度
func (bc *Blockchain) GetBlockSyncCommonBlockHeight(endBlockHeight int) (heigth, orphanCount, newblock int) {
	blocks, err := loader.GetLatestBlocks()
	if err != nil {
		panic("sync check, but failed.")
	}

	if endBlockHeight < 0 || endBlockHeight > len(bc.BlocksOfChainById) {
		endBlockHeight = len(bc.BlocksOfChainById)
	}

	orphanCount = 0
	for _, block := range blocks {
		blockIdHex := utils.HashString(block.BlockId)
		if _, ok := bc.BlocksOfChainById[blockIdHex]; ok {
			newblock = endBlockHeight - int(block.Height) - 1
			log.Printf("shoud sync block after height: %d, orphan: %d, new: %d",
				block.Height, orphanCount, newblock)
			return int(block.Height), orphanCount, newblock
		}
		orphanCount++
	}
	panic("sync check, but found more then 1000 orphan blocks.")
}
