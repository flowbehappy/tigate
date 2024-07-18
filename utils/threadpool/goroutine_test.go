package threadpool

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func benchmarkMassiveGoroutines(taskCount int, addCount int, b *testing.B) {
	for k := 0; k < b.N; k++ {
		var value atomic.Int64
		wg := sync.WaitGroup{}
		wg.Add(taskCount)

		start_run := sync.WaitGroup{}
		start_run.Add(1)

		chans := make([]chan struct{}, taskCount)

		for i := 0; i < taskCount; i++ {
			chans[i] = make(chan struct{})
			go func() {
				start_run.Wait()

				for i := 0; i < addCount; i++ {
					value.Add(1)
				}

				wg.Done()
			}()
		}

		start_run.Done()

		wg.Wait()
	}
}

func BenchmarkMassiveGoroutine1m(b *testing.B) {
	b.ReportAllocs()
	benchmarkMassiveGoroutines(1000000, 1000, b)
}

func BenchmarkMassiveGoroutine5m(b *testing.B) {
	benchmarkMassiveGoroutines(5000000, 1000, b)
}

type simpleTask struct {
	value *atomic.Int64
	count int
	wg    *sync.WaitGroup
}

func (t *simpleTask) Execute() (TaskStatus, time.Time) {
	for i := 0; i < t.count; i++ {
		t.value.Add(1)
	}
	t.wg.Done()
	return Done, time.Time{}
}

func benchmarkMassiveTasks(taskCount int, addCount int, b *testing.B) {
	for k := 0; k < b.N; k++ {
		var value atomic.Int64
		wg := sync.WaitGroup{}
		wg.Add(taskCount)
		taskScheduler := NewTaskScheduler("test", 0, -1)

		for i := 0; i < taskCount; i++ {
			taskScheduler.Submit(
				&simpleTask{
					value: &value,
					count: addCount,
					wg:    &wg,
				}, CPUTask, time.Now())
		}

		wg.Wait()
		taskScheduler.Stop()
	}
}

func BenchmarkMassiveTask1m(b *testing.B) {
	b.ReportAllocs()
	benchmarkMassiveTasks(1000000, 1000, b)
}

func BenchmarkMassiveTask5m(b *testing.B) {
	benchmarkMassiveTasks(5000000, 1000, b)
}

func main() {
	ch1 := make(chan int)
	ch2 := make(chan string)

	// Simulate sending data on channels
	go func() {
		time.Sleep(2 * time.Second)
		ch1 <- 42
	}()

	go func() {
		time.Sleep(1 * time.Second)
		ch2 <- "hello"
	}()

	cases := make([]reflect.SelectCase, 2)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch1)}
	cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch2)}

	for i := 0; i < 2; i++ {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			fmt.Println("Channel closed!")
			return
		}

		switch chosen {
		case 0:
			fmt.Printf("Received from ch1: %v\n", value.Int())
		case 1:
			fmt.Printf("Received from ch2: %v\n", value.String())
		}

		// Remove the handled case from the slice
		cases = append(cases[:chosen], cases[chosen+1:]...)
	}
}
