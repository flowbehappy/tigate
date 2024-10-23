package event

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestDDLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	ddlEvent := &DDLEvent{
		Version:      DDLEventVersion,
		DispatcherID: common.NewDispatcherID(),
		Type:         byte(ddlJob.Type),
		SchemaID:     ddlJob.SchemaID,
		TableID:      ddlJob.TableID,
		SchemaName:   ddlJob.SchemaName,
		TableName:    ddlJob.TableName,
		Query:        ddlJob.Query,
		TableInfo:    common.WrapTableInfo(ddlJob.SchemaID, ddlJob.SchemaName, ddlJob.BinlogInfo.TableInfo),
		FinishedTs:   ddlJob.BinlogInfo.FinishedTS,
	}

	data, err := ddlEvent.Marshal()
	require.Nil(t, err)

	reverseEvent := &DDLEvent{}
	err = reverseEvent.Unmarshal(data)
	require.Nil(t, err)
	require.Equal(t, ddlEvent, reverseEvent)
}
