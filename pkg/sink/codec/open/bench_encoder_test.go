package open

import (
	"fmt"
	"testing"
)

// func BenchmarkEncodeMessageKey(b *testing.B) {
// 	key := MessageKey{
// 		Ts:     1234567890,
// 		Schema: "test",
// 		Table:  "table",
// 		Type:   model.MessageTypeRow,
// 	}
// 	var ans [][]byte
// 	for i := 0; i < 10; i++ {
// 		key.Ts += 1
// 		res := key.Encode()
// 		ans = append(ans, res)
// 	}

// }

func BenchmarkEncodeMessage(b *testing.B) {
	var funcVec []func()

	for i := 0; i < 10; i++ {
		funcVec = append(funcVec, func() {
			fmt.Println("hello world", i)
		})
	}

	newFunc := func() {
		for _, f := range funcVec {
			f()
		}
	}

	funcVec = make([]func(), 0)

	newFunc()
}
