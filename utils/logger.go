package utils

import (
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	Log    *zap.Logger
	LogErr *zap.Logger

	LogBlk        *zap.Logger
	LogTx         *zap.Logger
	LogTxIn       *zap.Logger
	LogTxOut      *zap.Logger
	LogTxOutSpent *zap.Logger
)

type RowBinaryEncoder struct {
	*zapcore.MapObjectEncoder
}

var (
	_pool = buffer.NewPool()
	// Get retrieves a buffer from the pool, creating one if necessary.
	Get = _pool.Get
	// uleb128ResultCache = make([][]byte, 100*1024*1024)
)

func constructRowBinaryEncoder(config zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return &RowBinaryEncoder{
		// MapObjectEncoder: zapcore.NewMapObjectEncoder(),
	}, nil
}

var _rowbinaryEncoderPool = sync.Pool{
	New: func() interface{} {
		return &RowBinaryEncoder{
			// MapObjectEncoder: zapcore.NewMapObjectEncoder(),
		}
	},
}

func getRowBinaryEncoder() *RowBinaryEncoder {
	return _rowbinaryEncoderPool.Get().(*RowBinaryEncoder)
}

func putRowBinaryEncoder(e *RowBinaryEncoder) {
	_rowbinaryEncoderPool.Put(e)
}

func (enc *RowBinaryEncoder) Clone() zapcore.Encoder {
	return getRowBinaryEncoder()
}

func (enc *RowBinaryEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	myEnc := enc.Clone().(*RowBinaryEncoder)
	buf := _pool.Get()

	for _, f := range fields {
		switch f.Type {
		case zapcore.BinaryType:
			buf.Write(f.Interface.([]byte))
		case zapcore.BoolType:
			if f.Integer == 1 {
				binary.Write(buf, binary.LittleEndian, int8(1))
			} else {
				binary.Write(buf, binary.LittleEndian, int8(-1))
			}
		case zapcore.ByteStringType:
			var b []byte
			b = appendUleb128(b, uint64(len(f.Interface.([]byte))))
			buf.Write(b)
			buf.Write(f.Interface.([]byte))
		case zapcore.Uint64Type:
			binary.Write(buf, binary.LittleEndian, uint64(f.Integer))
		case zapcore.Uint32Type:
			binary.Write(buf, binary.LittleEndian, uint32(f.Integer))
		case zapcore.Uint16Type:
			binary.Write(buf, binary.LittleEndian, uint16(f.Integer))
		case zapcore.Uint8Type:
			binary.Write(buf, binary.LittleEndian, uint8(f.Integer))
		default:
			panic(fmt.Sprintf("unknown field type: %v", f))
		}
	}
	// buf.AppendByte('\n')
	putRowBinaryEncoder(myEnc)
	return buf, nil
}

func init() {
	zap.RegisterEncoder("row-binary", constructRowBinaryEncoder)
	// dumpEncoding := "console"
	dumpEncoding := "row-binary"

	// pathPrefix = "/home/jie/astudy/mongo"
	pathPrefix := "/data"
	// pathSurfix := ".mgo"
	pathSurfix := ".ch"

	Log, _ = zap.Config{
		Encoding:    "console",
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/output.log"},
	}.Build()

	LogErr, _ = zap.Config{
		Encoding:    "console",
		Level:       zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths: []string{"stderr"},
	}.Build()

	LogBlk, _ = zap.Config{
		Encoding:    dumpEncoding,
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/blk" + pathSurfix},
	}.Build()

	LogTx, _ = zap.Config{
		Encoding:    dumpEncoding,
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/tx" + pathSurfix},
	}.Build()

	LogTxIn, _ = zap.Config{
		Encoding:    dumpEncoding,
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/tx-in" + pathSurfix},
	}.Build()

	LogTxOut, _ = zap.Config{
		Encoding:    dumpEncoding,
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/tx-out" + pathSurfix},
	}.Build()

	LogTxOutSpent, _ = zap.Config{
		Encoding:    dumpEncoding,
		Level:       zap.NewAtomicLevelAt(zapcore.InfoLevel),
		OutputPaths: []string{pathPrefix + "/tx-out-spent" + pathSurfix},
	}.Build()

}

func SyncLog() {
	Log.Sync()
	LogErr.Sync()
	LogBlk.Sync()
	LogTx.Sync()
	LogTxIn.Sync()
	LogTxOut.Sync()
	LogTxOutSpent.Sync()
}
