package parser

import (
	"blkparser/loader"
	"blkparser/model"
	"blkparser/task"
	serialTask "blkparser/task/serial"
	"blkparser/utils"
	"encoding/hex"
	"log"
	"sync"
)

type Blockchain struct {
	Blocks        map[string]*model.Block // 所有区块
	BlocksOfChain map[string]*model.Block // 主链区块
	ParsedBlocks  map[string]bool         // 主链已分析区块
	MaxBlock      *model.Block
	GenesisBlock  *model.Block
	BlockData     *loader.BlockData
	m             sync.Mutex
}

func NewBlockchain(path string, magicHex string) (bc *Blockchain, err error) {
	magic, err := hex.DecodeString(magicHex)
	if err != nil {
		return nil, err
	}

	bc = new(Blockchain)
	bc.Blocks = make(map[string]*model.Block, 0)
	bc.ParsedBlocks = make(map[string]bool, 0)

	utils.LoadFromGobFile("./headers-list.gob", bc.Blocks)

	bc.BlockData, err = loader.NewBlockData(path, magic)
	if err != nil {
		return nil, err
	}
	return
}

// ParseLongestChain 两遍遍历区块。先获取header，再遍历区块
func (bc *Blockchain) ParseLongestChain(startBlockHeight, endBlockHeight int) {
	// 跳到指定高度的区块文件"附近"开始读取区块
	// 注意如果区块在文件中严重乱序(错位2个区块文件以上)则可能读取起始失败(分析进度不开始，内存占用持续增加)
	bc.SkipToBlockFileIdByBlockHeight(startBlockHeight)

	blocksReady := make(chan *model.Block, 256)
	done := make(chan struct{}, 1)

	//  解码区块，生产者
	go bc.InitLongestChainBlock(done, blocksReady, startBlockHeight, endBlockHeight)

	// 按顺序消费解码后的区块
	bc.ParseLongestChainBlock(blocksReady, startBlockHeight, endBlockHeight)
	log.Printf("consume ok")
	done <- struct{}{}
	for _ = range blocksReady {
	}
	// 最后分析执行
	task.ParseEnd()
}

// InitLongestChainBlock 解码区块，生产者
func (bc *Blockchain) InitLongestChainBlock(done chan struct{}, blocksReady chan *model.Block, startBlockHeight, endBlockHeight int) {
	var wg sync.WaitGroup
	parsers := make(chan struct{}, 28)
OUT:
	for {
		select {
		case <-done:
			break OUT
		default:
		}
		// 获取所有Block字节，不要求有序返回或属于主链
		// 但由于未分析的区块需要暂存，无序遍历会增加内存消耗
		rawblock, err := bc.BlockData.GetRawBlock()
		if err != nil {
			// log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}
		blockHash := utils.HashString(utils.GetShaString(rawblock[:80]))
		block, ok := bc.BlocksOfChain[blockHash]
		if !ok {
			// 若不是主链区块则跳过
			continue
		}
		// 不到高度的区块跳过
		if block.Height < startBlockHeight {
			continue
		}

		// 若已分析过则跳过
		if ok := bc.ParsedBlocks[blockHash]; ok {
			continue
		}
		bc.ParsedBlocks[blockHash] = true

		InitBlock(block, rawblock[:80])

		block.Raw = rawblock
		block.Size = uint32(len(rawblock))

		// 设置已经分析到的区块高度
		task.MaxBlockHeightParallel = block.Height
		parsers <- struct{}{}
		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()

			txs := NewTxs(block.Raw[80:])

			block.TxCnt = len(txs)
			block.Txs = txs

			processBlock := &model.ProcessBlock{
				Height:         block.Height,
				UtxoMap:        make(map[string]model.CalcData, 1),
				UtxoMissingMap: make(map[string]bool, 1),
			}
			block.ParseData = processBlock

			// 超过高度的不分析。但不能在此退出，因为区块文件是乱序的
			if endBlockHeight < 0 || block.Height < endBlockHeight {
				// 先并行分析区块。可执行一些区块内的独立预处理任务，不同区块会并行乱序执行
				task.ParseBlockParallel(block)
			}

			block.Raw = nil
			blocksReady <- block
			<-parsers
		}(block)
	}
	wg.Wait()

	close(blocksReady)
	log.Printf("produce ok")
}

// ParseLongestChainBlock 按顺序消费解码后的区块
func (bc *Blockchain) ParseLongestChainBlock(blocksReady chan *model.Block, startBlockHeight, maxBlockHeight int) {
	blocksTotal := len(bc.BlocksOfChain)
	if maxBlockHeight < 0 || maxBlockHeight > blocksTotal {
		maxBlockHeight = blocksTotal
	}

	if startBlockHeight >= maxBlockHeight {
		return
	}

	nextBlockHeight := startBlockHeight
	buffCount := 0
	blockParseBufferBlock := make([]*model.Block, maxBlockHeight)
	for block := range blocksReady {
		// 暂存block
		if block.Height < maxBlockHeight {
			blockParseBufferBlock[block.Height] = block
			buffCount++
		}

		// 注意如果在开始分析之前暂存的量非常大则可能是异常情况
		if nextBlockHeight == startBlockHeight && buffCount > 1000 {
			panic("too many buff blocks. starting block may missing")
		}

		// 按序
		if block.Height != nextBlockHeight {
			continue
		}
		for nextBlockHeight < maxBlockHeight {
			block = blockParseBufferBlock[nextBlockHeight]
			if block == nil { // 检查是否准备好
				break
			}

			// 再串行分析区块。可执行一些严格要求按序处理的任务，区块会串行依次执行
			// 当串行执行到某个区块时，这个区块一定运行完毕了相应的并行预处理任务
			task.ParseBlockSerial(block,
				startBlockHeight+buffCount-nextBlockHeight,
				maxBlockHeight)

			block.Txs = nil
			nextBlockHeight++
		}
		if nextBlockHeight >= maxBlockHeight {
			break
		}
	}
}

// SkipToBlockFileIdByBlockHeight 跳到height高度的区块文件附近开始读取区块
func (bc *Blockchain) SkipToBlockFileIdByBlockHeight(height int) {
	for _, blk := range bc.Blocks {
		if blk.Height == height {
			//回撤2个区块文件开始扫描
			// 在区块文件严重乱序情况下，可能导致漏过开始区块，分析将失败
			skipTo := 0
			if blk.FileIdx > 2 {
				skipTo = blk.FileIdx - 2
			}
			bc.BlockData.SkipTo(skipTo, 0)
			return
		}
	}
	bc.BlockData.SkipTo(0, 0)
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
		utils.DumpToGobFile("./headers-list.gob", bc.Blocks)
	}
	// 如果遗漏中间block header，可能导致最长链无法延长
	// 造成 bc.BlocksOfChain 中区块数量远小于 bc.Blocks
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
		}(rawblock, bc.BlockData.CurrentId, bc.BlockData.Offset)

		// header speed
		serialTask.ParseBlockSpeed(0, idx, idx, 0, 0)
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
	bc.BlocksOfChain = make(map[string]*model.Block, 0)
	block := bc.MaxBlock
	for {
		// 由于之前的高度是从1开始，现在统一减一
		block.Height -= 1
		bc.BlocksOfChain[block.HashHex] = block
		// 设置genesis
		bc.GenesisBlock = block
		var ok bool
		block, ok = bc.Blocks[block.ParentHex]
		if !ok {
			break
		}
	}
	log.Printf("genesis block: %s", bc.GenesisBlock.HashHex)
	log.Printf("chain blocks count: %d", len(bc.BlocksOfChain))

	// 孤块太多，可能出现区块头遗漏，需要更多的区块文件扫描回撤
	// 见：InitLongestChainHeader
	if len(bc.Blocks) > len(bc.BlocksOfChain)+1000 {
		panic("too many orphan blocks. block header may missing")
	}
}

// GetBlockSyncCommonBlockHeight 获取区块同步起始的共同区块高度
func (bc *Blockchain) GetBlockSyncCommonBlockHeight(endBlockHeight int) (heigth, orphanCount int) {
	blocks, err := loader.GetLatestBlocks()
	if err != nil {
		panic("sync check, but failed.")
	}

	if endBlockHeight < 0 || endBlockHeight > len(bc.BlocksOfChain) {
		endBlockHeight = len(bc.BlocksOfChain)
	}

	orphanCount = 0
	for _, block := range blocks {
		blockIdHex := utils.HashString(block.BlockId)
		if _, ok := bc.BlocksOfChain[blockIdHex]; ok {
			log.Printf("shoud sync block after height: %d, new: %d, orphan: %d",
				block.Height, endBlockHeight-int(block.Height)-1, orphanCount)
			return int(block.Height), orphanCount
		}
		orphanCount++
	}
	panic("sync check, but found more then 1000 orphan blocks.")
}
