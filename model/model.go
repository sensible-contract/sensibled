package model

import (
	"encoding/binary"

	"go.uber.org/multierr"
	"go.uber.org/zap/zapcore"
)

type Tx struct {
	HashHex   string // 32
	Hash      Bytes  // 32
	Size      uint32
	WitOffset uint
	LockTime  uint32
	Version   uint32
	TxInCnt   uint32
	TxOutCnt  uint32
	TxIns     TxIns
	TxOuts    TxOuts
	TxWits    []*TxWit
}

type TxIn struct {
	InputHashHex string // 32
	InputHash    Bytes  // 32
	InputVout    uint32
	ScriptSig    Bytes
	Sequence     uint32

	// other:
	InputOutpointKey string // 32 + 4
	InputOutpoint    Bytes  // 32 + 4
	InputPoint       Bytes  // 32 + 4
}

func (t *TxIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("t", t.InputHashHex)
	enc.AddUint32("i", t.InputVout)
	enc.AddObject("s", t.ScriptSig)
	return nil
}

type Bytes []byte

func (b Bytes) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddBinary("$binary", b)
	enc.AddString("$type", "05")
	return nil
}

type TxOut struct {
	Value    uint64
	Pkscript Bytes

	// other:
	AddressPkh           Bytes
	GenesisId            Bytes
	Outpoint             Bytes  // 32 + 4
	OutpointKey          string // 32 + 4
	LockingScriptType    Bytes
	LockingScriptTypeHex string
	LockingScriptMatch   bool
}

func (t *TxOut) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint64("v", t.Value)
	enc.AddObject("s", t.Pkscript)
	return nil
}

type TxIns []*TxIn

func (tt TxIns) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	var err error
	for i := range tt {
		err = multierr.Append(err, arr.AppendObject(tt[i]))
	}
	return err
}

type TxOuts []*TxOut

func (tt TxOuts) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	var err error
	for i := range tt {
		err = multierr.Append(err, arr.AppendObject(tt[i]))
	}
	return err
}

type TxWit struct {
	Value    uint64
	Pkscript []byte

	// other:
	Addr string
}

////////////////
type Block struct {
	Raw        []byte
	Hash       Bytes  // 32 bytes
	HashHex    string // 32 bytes
	FileIdx    int
	FileOffset int
	Height     int
	Txs        []*Tx
	Version    uint32
	MerkleRoot Bytes // 32 bytes
	BlockTime  uint32
	Bits       uint32
	Nonce      uint32
	Size       uint32
	TxCnt      int
	Parent     Bytes  // 32 bytes
	ParentHex  string // 32 bytes
	NextHex    string // 32 bytes
	ParseData  *ProcessBlock
}

type BlockCache struct {
	Hash       Bytes // 32 bytes
	FileIdx    int
	FileOffset int
	Parent     Bytes // 32 bytes
}

////////////////
type ProcessBlock struct {
	Height           uint32
	NewUtxoDataMap   map[string]CalcData
	SpentUtxoKeysMap map[string]bool
	SpentUtxoDataMap map[string]CalcData
}

type CalcData struct {
	UTxid       Bytes
	Vout        uint32
	BlockHeight uint32
	TxIdx       uint64
	AddressPkh  Bytes
	GenesisId   Bytes
	Value       uint64
	ScriptType  Bytes
	Script      Bytes
}

func (d *CalcData) Marshal(buf []byte) {
	binary.LittleEndian.PutUint32(buf, d.BlockHeight) // 4
	binary.LittleEndian.PutUint64(buf[4:], d.TxIdx)   // 8
	binary.LittleEndian.PutUint64(buf[12:], d.Value)  // 8
	// copy(buf[12:], d.AddressPkh)                      // 20
	// copy(buf[32:], d.GenesisId)                       // 20
	// copy(buf[60:], d.ScriptType)                      // 32
	copy(buf[20:], d.Script) // n
}

func (d *CalcData) Unmarshal(buf []byte) {
	d.BlockHeight = binary.LittleEndian.Uint32(buf[:4]) // 4
	d.TxIdx = binary.LittleEndian.Uint64(buf[4:12])     // 8
	d.Value = binary.LittleEndian.Uint64(buf[12:20])    // 8
	// copy(d.AddressPkh, buf[12:32])                      // 20
	// copy(d.GenesisId, buf[32:52])                       // 20
	// copy(d.ScriptType, buf[60:92])                      // 32
	d.Script = buf[20:]
	// d.Script = make([]byte, len(buf)-20)
	// copy(d.Script, buf[20:]) // n
}