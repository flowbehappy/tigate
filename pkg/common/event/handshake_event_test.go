package event

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestHandshakeEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job(createTableSQL)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	e := NewHandshakeEvent(common.NewDispatcherID(), 456, 789, dmlEvent.TableInfo)
	data, err := e.Marshal()
	require.NoError(t, err)

	e2 := &HandshakeEvent{}
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}
