package model

import (
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
	Height         int
	UtxoMap        map[string]CalcData
	UtxoMissingMap map[string]bool
}

type CalcData struct {
	BlockHeight int
	AddressPkh  Bytes
	GenesisId   Bytes
	Value       uint64
	ScriptType  Bytes
	Script      Bytes
}
