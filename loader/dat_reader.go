package loader

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sensibled/logger"
	"sync"

	"go.uber.org/zap"
)

type BlockData struct {
	StripMode   bool
	Path        string
	Magic       []byte
	CurrentFile *os.File
	CurrentId   int
	LastFileId  int
	LastOffset  int
	Offset      int
	m           sync.Mutex
}

func NewBlockData(stripMode bool, path string, magic []byte) (bf *BlockData) {
	bf = new(BlockData)
	bf.Path = path
	bf.Magic = magic
	bf.CurrentId = -1
	bf.StripMode = stripMode
	return
}

func (bf *BlockData) GetRawBlockHeader() (rawblockheader []byte, err error) {
	if bf.StripMode {
		return bf.fetchNextStripedBlockHeader()
	}
	return bf.nextRawBlockData(true)
}

func (bf *BlockData) GetRawBlock() (rawblock []byte, err error) {
	if bf.StripMode {
		rawblock, err = os.ReadFile(bf.getBlockFileName(bf.Path, bf.CurrentId))
		if err == nil {
			bf.LastFileId = bf.CurrentId
			bf.CurrentId += 1
		}
		return
	}
	return bf.nextRawBlockData(false)
}

func (bf *BlockData) fetchNextStripedBlockHeader() (rawblock []byte, err error) {
	readOffset := 0
	readSize := 89

	rawblock = make([]byte, 80+9) // block header + txn

	fd, err := os.Open(bf.getBlockFileName(bf.Path, bf.CurrentId))
	if err != nil {
		return
	}
	defer fd.Close()

	// read until to readSize
	for {
		var n int
		n, err = fd.Read(rawblock[readOffset:])
		if err != nil {
			return
		}
		readOffset += n
		if readOffset == readSize {
			break
		}
	}

	bf.LastFileId = bf.CurrentId
	bf.CurrentId += 1
	return
}

func (bf *BlockData) nextRawBlockData(skipTxs bool) (rawblock []byte, err error) {
	bf.m.Lock()
	defer bf.m.Unlock()

	rawblock, err = bf.fetchNextBlock(skipTxs)
	if err != nil {
		newblkfile, err2 := os.Open(bf.getBlockFileName(bf.Path, bf.CurrentId+1))
		if err2 != nil {
			return nil, err2
		}

		bf.CurrentId++
		bf.CurrentFile.Close()
		bf.CurrentFile = newblkfile
		bf.Offset = 0
		bf.LastFileId = bf.CurrentId
		rawblock, err = bf.fetchNextBlock(skipTxs)
	}
	return rawblock, nil
}

func (bf *BlockData) fetchNextBlock(skipTxs bool) (rawblock []byte, err error) {
	bf.LastOffset = bf.Offset

	buf := [4]byte{}
	_, err = bf.CurrentFile.Read(buf[:])
	if err != nil {
		// logger.Log.Info("fetchNextBlock done", zap.Error(err))
		return
	}
	bf.Offset += 4

	if !bytes.Equal(buf[:], bf.Magic) {
		err = errors.New("Bad magic")
		logger.Log.Info("fetchNextBlock done: Bad magic",
			zap.Int("fid", bf.CurrentId),
			zap.Int("offset", bf.Offset),
			zap.String("buf", hex.EncodeToString(buf[:])),
			zap.String("magic", hex.EncodeToString(bf.Magic[:])),
		)
		return
	}

	_, err = bf.CurrentFile.Read(buf[:])
	if err != nil {
		return
	}
	bf.Offset += 4

	blocksize := binary.LittleEndian.Uint32(buf[:])
	readOffset := 0
	readSize := 0

	if skipTxs {
		rawblock = make([]byte, 80+9) // block header + txn
		readSize = 89
	} else {
		rawblock = make([]byte, blocksize)
		readSize = int(blocksize)
	}

	// read until to readSize
	for {
		var n int
		n, err = bf.CurrentFile.Read(rawblock[readOffset:])
		if err != nil {
			return
		}
		readOffset += n
		if readOffset == readSize {
			break
		}
	}
	if skipTxs {
		_, err = bf.CurrentFile.Seek(int64(blocksize-80-9), os.SEEK_CUR)
		if err != nil {
			return
		}
	}
	bf.Offset += int(blocksize)
	return
}

// Convenience method to skip directly to the given blkfile / offset,
// you must take care of the height
func (bf *BlockData) SkipTo(blkId int, offset int) (err error) {
	bf.m.Lock()
	defer bf.m.Unlock()

	if bf.StripMode {
		bf.CurrentId = blkId
		bf.LastFileId = bf.CurrentId
		bf.Offset = 0
		return
	}

	if bf.CurrentId != blkId {
		f, err := os.Open(bf.getBlockFileName(bf.Path, blkId))
		if err != nil {
			return err
		}
		bf.CurrentFile.Close()
		bf.CurrentFile = f
		bf.CurrentId = blkId
		bf.LastFileId = bf.CurrentId
		bf.Offset = 0
	}

	if bf.Offset != offset {
		_, err = bf.CurrentFile.Seek(int64(offset), 0)
		if err != nil {
			return err
		}
		bf.Offset = offset
	}
	return
}

func (bf *BlockData) getBlockFileName(path string, id int) string {
	if bf.StripMode {
		return fmt.Sprintf("%s/%04d/%07d", path, id/1000, id)
	}
	return fmt.Sprintf("%s/blk%05d.dat", path, id)
}
