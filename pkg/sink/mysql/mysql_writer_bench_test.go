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

// cpu: Apple M1 Pro
// BenchmarkPrepareDMLs/Events-1-10         	14434258	        72.41 ns/op	     256 B/op	       1 allocs/op
// BenchmarkPrepareDMLs/Events-10-10        	 2088097	       631.0 ns/op	    2565 B/op	      10 allocs/op
// BenchmarkPrepareDMLs/Events-100-10       	  228222	      5796 ns/op	   25647 B/op	     100 allocs/op
// BenchmarkPrepareDMLs/Events-1000-10      	   20516	     72573 ns/op	  256549 B/op	    1004 allocs/op
// BenchmarkPrepareDMLs/Events-10000-10     	    2203	    604690 ns/op	 2565368 B/op	   10045 allocs/op
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
