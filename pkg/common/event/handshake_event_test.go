package event

import (
	"testing"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestHandshakeEvent(t *testing.T) {
	e := NewHandshakeEvent(common.NewDispatcherID(), 456, 789)
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, 1+8+8+16)

	e2 := &HandshakeEvent{}
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e, e2)
}
