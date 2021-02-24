package loader

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

type BlockData struct {
	Path        string
	Magic       []byte
	CurrentFile *os.File
	CurrentId   int
	Offset      int
	m           sync.Mutex
}

func NewBlockData(path string, magic []byte) (bf *BlockData, err error) {
	bf = new(BlockData)
	bf.Path = path
	bf.Magic = magic
	bf.CurrentId = 0
	bf.Offset = 0
	f, err := os.Open(blkfilename(path, 0))
	if err != nil {
		return
	}

	bf.CurrentFile = f
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
	buf := [4]byte{}
	_, err = bf.CurrentFile.Read(buf[:])
	if err != nil {
		// log.Printf("read failed: %v", err)
		return
	}
	bf.Offset += 4

	if !bytes.Equal(buf[:], bf.Magic) {
		err = errors.New("Bad magic")
		// log.Printf("read blk%d[%d] failed: %v, %v != %v", bf.CurrentId, bf.Offset, err, buf[:], bf.Magic[:])
		return
	}

	_, err = bf.CurrentFile.Read(buf[:])
	if err != nil {
		return
	}
	bf.Offset += 4

	blocksize := binary.LittleEndian.Uint32(buf[:])

	if skipTxs {
		rawblock = make([]byte, 80)
	} else {
		rawblock = make([]byte, blocksize)
	}
	_, err = bf.CurrentFile.Read(rawblock[:])
	if err != nil {
		return
	}

	if skipTxs {
		_, err = bf.CurrentFile.Seek(int64(blocksize-80), os.SEEK_CUR)
		if err != nil {
			return
		}
	}
	bf.Offset += int(blocksize)
	return
}

// Convenience method to skip directly to the given blkfile / offset,
// you must take care of the height
func (bf *BlockData) SkipTo(blkId int, offset int64) (err error) {
	bf.m.Lock()
	defer bf.m.Unlock()

	bf.CurrentId = blkId
	bf.Offset = 0
	f, err := os.Open(blkfilename(bf.Path, blkId))
	if err != nil {
		return
	}
	bf.CurrentFile.Close()
	bf.CurrentFile = f
	_, err = bf.CurrentFile.Seek(offset, 0)
	bf.Offset = int(offset)
	return
}

func blkfilename(path string, id int) string {
	return fmt.Sprintf("%s/blk%05d.dat", path, id)
}
