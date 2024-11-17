package mysql

import (
	"fmt"
	"testing"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func BenchmarkPrepareDMLs(b *testing.B) {
	log.SetLevel(zap.ErrorLevel)
	writer := &MysqlWriter{
		cfg: &MysqlConfig{
			SafeMode: false,
		},
	}

	helper := pevent.NewEventTestHelper(b)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job(preCreateTableSQL)

	event := helper.DML2Event("test", "t", preInsertDataSQL)
	require.NotNil(b, event)
	insert, ok := event.GetNextRow()
	require.True(b, ok)
	require.NotNil(b, insert)

	// 定义不同的事件数量进行测试
	eventCounts := []int{1, 10, 100, 1000, 10000}

	for _, count := range eventCounts {
		events := make([]*commonEvent.DMLEvent, 0, count)
		for i := 0; i < count; i++ {
			events = append(events, event)
		}

		b.Run(fmt.Sprintf("Events-%d", count), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dmls, err := writer.prepareDMLs(events)
				if err != nil {
					b.Fatal(err)
				}
				// Clean up
				putDMLs(dmls)
			}
		})
	}
}
