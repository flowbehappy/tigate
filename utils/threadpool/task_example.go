package threadpool

import "time"

type OnceTask struct{}

func (t *OnceTask) Execute() time.Time {
	return time.Time{}
}

type RepeatedTask struct {
	taskHandle *TaskHandle
}

func NewRepeatedTask(ts ThreadPool) *RepeatedTask {
	t := &RepeatedTask{}
	t.taskHandle = ts.Submit(t, time.Now().Add(1*time.Second))
	return t
}

func (t *RepeatedTask) Execute() time.Time {
	return time.Now().Add(1 * time.Second)
}

func (t *RepeatedTask) Cancel() {
	t.taskHandle.Cancel()
}

func RunSomethingRepeatedly() {
	tp := NewThreadPoolDefault()

	times := 0
	tp.SubmitFunc(func() time.Time {
		// Do something 1000 times

		times++
		if times == 1000 {
			return time.Time{}
		} else {
			return time.Now().Add(1 * time.Nanosecond)
		}
	}, time.Now())

	tp.Stop()
}
