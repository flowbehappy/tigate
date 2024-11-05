package threadpool

import (
	"sync"
	"sync/atomic"
	"time"
)

type threadPoolImpl struct {
	threadCount int

	pendingTaskChan chan *scheduledTask

	reactor *waitReactor

	stopSignal chan struct{}
	wg         sync.WaitGroup
	hasClosed  atomic.Bool
}

func newThreadPoolImpl(threadCount int) *threadPoolImpl {
	tp := &threadPoolImpl{
		threadCount:     threadCount,
		pendingTaskChan: make(chan *scheduledTask, threadCount),
		stopSignal:      make(chan struct{}),
	}
	tp.reactor = newWaitReactor(tp)

	tp.wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go tp.executeTasks()
	}

	return tp
}

func (t *threadPoolImpl) Submit(task Task, next time.Time) *TaskHandle {
	st := &scheduledTask{
		task: task,
	}
	t.reactor.newTaskChan <- taskAndTime{st, next}
	return &TaskHandle{st}
}

func (t *threadPoolImpl) SubmitFunc(task FuncTask, next time.Time) *TaskHandle {
	return t.Submit(&funcTaskImpl{task}, next)
}

func (t *threadPoolImpl) Stop() {
	if t.hasClosed.CompareAndSwap(false, true) {
		close(t.stopSignal)
		close(t.reactor.stopSignal)
	}
	t.wg.Wait()
	t.reactor.wg.Wait()
}

func (t *threadPoolImpl) executeTasks() {
	defer t.wg.Done()

	for {
		select {
		case <-t.stopSignal:
			return
		case task := <-t.pendingTaskChan:
			// Canceled task will not be executed and dropped.
			if !task.isCanceled() {
				next := task.task.Execute()
				if !next.IsZero() {
					t.reactor.newTaskChan <- taskAndTime{task, next}
				}
			}
		}
	}
}
