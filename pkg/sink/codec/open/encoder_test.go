package open

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/mounter"
	newcommon "github.com/flowbehappy/tigate/pkg/sink/codec/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec/encoder"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	ticonfig "github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// normal message 1
// normal message multiple to merge
// large message (with handle / claim)

func readByteToUint(b []byte) uint64 {
	var value uint64
	log.Info("read byte", zap.Any("b", b[:]))
	buf := bytes.NewReader(b[:])
	err := binary.Read(buf, binary.BigEndian, &value)
	if err != nil {
		log.Error("Error reading int64 from byte slice:", zap.Any("error", err))
		return 0
	}
	return value
}

func TestEncoderOneMessage(t *testing.T) {
	ctx := context.Background()
	config := newcommon.NewConfig(config.ProtocolOpen)
	batchEncoder, err := NewBatchEncoder(ctx, config)
	require.NoError(t, err)

	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	count := 0

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() { count += 1 }}

	err = batchEncoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	messages := batchEncoder.Build()

	require.Equal(t, 1, len(messages))
	require.Equal(t, 1, messages[0].GetRowsCount())

	message := messages[0]
	require.Equal(t, uint64(encoder.BatchVersion1), readByteToUint(message.Key[:8]))
	require.Equal(t, uint64(len(message.Key[16:])), readByteToUint(message.Key[8:16]))
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(message.Key[16:]))

	require.Equal(t, uint64(len(message.Value[8:])), readByteToUint(message.Value[:8]))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":123}}}`, string(message.Value[8:]))

	message.Callback()

	require.Equal(t, 1, count)
}

func TestEncoderMultipleMessage(t *testing.T) {
	ctx := context.Background()
	config := newcommon.NewConfig(config.ProtocolOpen)
	config = config.WithMaxMessageBytes(400)

	batchEncoder, err := NewBatchEncoder(ctx, config)
	require.NoError(t, err)

	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`, `insert into test.t values (2, 223)`, `insert into test.t values (3, 333)`)

	count := 0

	for {
		insertRow, ok := dmlEvent.GetNextRow()
		if !ok {
			break
		}

		insertRowEvent := &common.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       1,
			Event:          insertRow,
			ColumnSelector: common.NewDefaultColumnSelector(),
			Callback:       func() { count += 1 }}

		err = batchEncoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
		require.NoError(t, err)
	}

	messages := batchEncoder.Build()

	require.Equal(t, 2, len(messages))
	require.Equal(t, 2, messages[0].GetRowsCount())
	require.Equal(t, 1, messages[1].GetRowsCount())

	// message1
	message1 := messages[0]
	require.Equal(t, uint64(encoder.BatchVersion1), readByteToUint(message1.Key[:8]))
	length1 := readByteToUint(message1.Key[8:16])
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(message1.Key[16:16+length1]))
	length2 := readByteToUint(message1.Key[16+length1 : 24+length1])
	require.Equal(t, uint64(len(message1.Key[24+length1:])), length2)
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(message1.Key[24+length1:]))

	length3 := readByteToUint(message1.Value[:8])
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1},"b":{"t":3,"f":65,"v":123}}}`, string(message1.Value[8:8+length3]))
	length4 := readByteToUint(message1.Value[8+length3 : 16+length3])
	require.Equal(t, uint64(len(message1.Value[16+length3:])), length4)
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":2},"b":{"t":3,"f":65,"v":223}}}`, string(message1.Value[16+length3:]))

	// message2
	message2 := messages[1]
	require.Equal(t, uint64(encoder.BatchVersion1), readByteToUint(message2.Key[:8]))
	require.Equal(t, uint64(len(message2.Key[16:])), readByteToUint(message2.Key[8:16]))
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(message2.Key[16:]))

	require.Equal(t, uint64(len(message2.Value[8:])), readByteToUint(message2.Value[:8]))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":3},"b":{"t":3,"f":65,"v":333}}}`, string(message2.Value[8:]))

	for _, message := range messages {
		message.Callback()
	}

	require.Equal(t, 3, count)
}

func TestLargeMessage(t *testing.T) {
	ctx := context.Background()
	config := newcommon.NewConfig(config.ProtocolOpen)
	config = config.WithMaxMessageBytes(100)
	batchEncoder, err := NewBatchEncoder(ctx, config)
	require.NoError(t, err)

	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	count := 0

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() { count += 1 }}

	err = batchEncoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.ErrorIs(t, err, cerror.ErrMessageTooLarge)
}

func TestLargeMessageWithHandle(t *testing.T) {
	ctx := context.Background()
	config := newcommon.NewConfig(config.ProtocolOpen)
	config = config.WithMaxMessageBytes(150)
	config.LargeMessageHandle.LargeMessageHandleOption = ticonfig.LargeMessageHandleOptionHandleKeyOnly
	batchEncoder, err := NewBatchEncoder(ctx, config)
	require.NoError(t, err)

	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	err = batchEncoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	messages := batchEncoder.Build()

	require.Equal(t, 1, len(messages))
	require.Equal(t, 1, messages[0].GetRowsCount())

	message := messages[0]
	require.Equal(t, uint64(encoder.BatchVersion1), readByteToUint(message.Key[:8]))
	require.Equal(t, uint64(len(message.Key[16:])), readByteToUint(message.Key[8:16]))
	require.Equal(t, `{"ts":1,"scm":"test","tbl":"t","t":1}`, string(message.Key[16:]))

	require.Equal(t, uint64(len(message.Value[8:])), readByteToUint(message.Value[:8]))
	require.Equal(t, `{"u":{"a":{"t":1,"h":true,"f":11,"v":1}}}`, string(message.Value[8:]))
}

func TestLargeMessageWithoutHandle(t *testing.T) {
	ctx := context.Background()
	config := newcommon.NewConfig(config.ProtocolOpen)
	config = config.WithMaxMessageBytes(150)
	config.LargeMessageHandle.LargeMessageHandleOption = ticonfig.LargeMessageHandleOptionHandleKeyOnly
	batchEncoder, err := NewBatchEncoder(ctx, config)
	require.NoError(t, err)

	helper := mounter.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &common.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: common.NewDefaultColumnSelector(),
		Callback:       func() {}}

	err = batchEncoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.ErrorIs(t, err, cerror.ErrOpenProtocolCodecInvalidData)
}
