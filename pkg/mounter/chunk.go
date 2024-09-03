package mounter

import (
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"

	"go.uber.org/zap"
)

type chunkDecoder struct {
	tableID int64
	version uint64
	decoder *rowcodec.ChunkDecoder
}

func (c *chunkDecoder) decode(value []byte, handle kv.Handle, chk *chunk.Chunk) error {
	return c.decoder.DecodeToChunk(value, handle, chk)
}

func (m *mounter) rawKVToChunkV2(value []byte, tableInfo *common.TableInfo, chk *chunk.Chunk, handle kv.Handle) error {
	if len(value) == 0 {
		return nil
	}
	v, ok := m.chunkDecoders.Load(tableInfo.ID)
	d := v.(*chunkDecoder)
	if ok {
		if d.version != tableInfo.UpdateTS {
			m.chunkDecoders.Delete(tableInfo.ID)
			d = nil
		} else {
			err := d.decode(value, handle, chk)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	handleColIDs, _, reqCols := tableInfo.GetRowColInfos()
	// This function is used to set the default value for the column that
	// is not in the raw data.
	defVal := func(i int, chk *chunk.Chunk) error {
		if reqCols[i].ID < 0 {
			// model.ExtraHandleID, ExtraPidColID, ExtraPhysTblID... etc
			// Don't set the default value for that column.
			chk.AppendNull(i)
			return nil
		}
		ci, ok := tableInfo.GetColumnInfo(reqCols[i].ID)
		if !ok {
			log.Panic("column not found", zap.Int64("columnID", reqCols[i].ID))
		}

		colDatum, _, _, warn, err := getDefaultOrZeroValue(ci, m.tz)
		if err != nil {
			return err
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", ci.Name.String()))
		}
		chk.AppendDatum(i, &colDatum)
		return nil
	}
	decoder := rowcodec.NewChunkDecoder(reqCols, handleColIDs, defVal, m.tz)
	d = &chunkDecoder{
		tableID: tableInfo.ID,
		version: tableInfo.UpdateTS,
		decoder: decoder,
	}
	// cache it for later use
	m.chunkDecoders.Store(tableInfo.ID, d)
	err := d.decode(value, handle, chk)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *mounter) rawKVToChunkV1(value []byte, tableInfo *common.TableInfo, chk *chunk.Chunk, handle kv.Handle) error {
	if len(value) == 0 {
		return nil
	}
	pkCols := tables.TryGetCommonPkColumnIds(tableInfo.TableInfo)
	prefixColIDs := tables.PrimaryPrefixColumnIDs(tableInfo.TableInfo)
	colID2CutPos := make(map[int64]int)
	for _, col := range tableInfo.TableInfo.Columns {
		if _, ok := colID2CutPos[col.ID]; !ok {
			colID2CutPos[col.ID] = len(colID2CutPos)
		}
	}
	// TODO: handle old value
	cutVals, err := tablecodec.CutRowNew(value, colID2CutPos)
	if err != nil {
		return err
	}
	if cutVals == nil {
		cutVals = make([][]byte, len(colID2CutPos))
	}
	decoder := codec.NewDecoder(chk, m.tz)
	for i, col := range tableInfo.TableInfo.Columns {
		if col.IsVirtualGenerated() {
			chk.AppendNull(i)
			continue
		}
		ok, err := tryDecodeFromHandle(tableInfo.TableInfo, i, col, handle, chk, decoder, pkCols, prefixColIDs)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		cutPos := colID2CutPos[col.ID]
		if len(cutVals[cutPos]) == 0 {
			colInfo := tableInfo.TableInfo.Columns[i]
			d, _, _, _, err1 := getDefaultOrZeroValue(colInfo, m.tz)
			if err1 != nil {
				return err1
			}
			chk.AppendDatum(i, &d)
			continue
		}
		_, err = decoder.DecodeOne(cutVals[cutPos], i, &col.FieldType)
		if err != nil {
			return err
		}
	}
	return nil
}

func tryDecodeFromHandle(tblInfo *timodel.TableInfo, schemaColIdx int, col *timodel.ColumnInfo, handle kv.Handle, chk *chunk.Chunk,
	decoder *codec.Decoder, pkCols []int64, prefixColIDs []int64) (bool, error) {
	if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.FieldType.GetFlag()) {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if col.ID == timodel.ExtraHandleID {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if types.NeedRestoredData(&col.FieldType) {
		return false, nil
	}
	// Try to decode common handle.
	if mysql.HasPriKeyFlag(col.FieldType.GetFlag()) {
		for i, hid := range pkCols {
			if col.ID == hid && notPKPrefixCol(hid, prefixColIDs) {
				_, err := decoder.DecodeOne(handle.EncodedCol(i), schemaColIdx, &col.FieldType)
				if err != nil {
					return false, errors.Trace(err)
				}
				return true, nil
			}
		}
	}
	return false, nil
}

func notPKPrefixCol(colID int64, prefixColIDs []int64) bool {
	for _, pCol := range prefixColIDs {
		if pCol == colID {
			return false
		}
	}
	return true
}
