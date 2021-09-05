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
	Path        string
	Magic       []byte
	CurrentFile *os.File
	CurrentId   int
	LastOffset  int
	Offset      int
	m           sync.Mutex
}

func NewBlockData(path string, magic []byte) (bf *BlockData) {
	bf = new(BlockData)
	bf.Path = path
	bf.Magic = magic
	bf.CurrentId = -1
	return
}

// GetRawBlockHeader
func (bf *BlockData) GetRawBlockHeader() (rawblockheader []byte, err error) {
	return bf.NextRawBlockData(true)
}

func (bf *BlockData) GetRawBlock() (rawblock []byte, err error) {
	return bf.NextRawBlockData(false)
}

func (bf *BlockData) NextRawBlockData(skipTxs bool) (rawblock []byte, err error) {
	bf.m.Lock()
	defer bf.m.Unlock()

	rawblock, err = bf.FetchNextBlock(skipTxs)
	if err != nil {
		newblkfile, err2 := os.Open(blkfilename(bf.Path, bf.CurrentId+1))
		if err2 != nil {
			return nil, err2
		}

		bf.CurrentId++
		bf.CurrentFile.Close()
		bf.CurrentFile = newblkfile
		bf.Offset = 0
		rawblock, err = bf.FetchNextBlock(skipTxs)
	}
	return rawblock, nil
}

func (bf *BlockData) FetchNextBlock(skipTxs bool) (rawblock []byte, err error) {
	bf.LastOffset = bf.Offset

	buf := [4]byte{}
	_, err = bf.CurrentFile.Read(buf[:])
	if err != nil {
		logger.Log.Info("FetchNextBlock done", zap.Error(err))
		return
	}
	bf.Offset += 4

	if !bytes.Equal(buf[:], bf.Magic) {
		err = errors.New("Bad magic")
		logger.Log.Info("FetchNextBlock done: Bad magic",
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

	if bf.CurrentId != blkId {
		f, err := os.Open(blkfilename(bf.Path, blkId))
		if err != nil {
			return err
		}
		bf.CurrentFile.Close()
		bf.CurrentFile = f
		bf.CurrentId = blkId
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

func blkfilename(path string, id int) string {
	return fmt.Sprintf("%s/blk%05d.dat", path, id)
}
