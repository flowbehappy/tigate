package threadpool

import "time"

type OnceTask struct{}

func (t *OnceTask) Execute() (TaskStatus, time.Time) {
	return Done, time.Time{}
}

type RepeatedTask struct {
	taskHandle *TaskHandle
}

func NewRepeatedTask(ts *TaskScheduler) *RepeatedTask {
	t := &RepeatedTask{}
	t.taskHandle = ts.Submit(t, CPUTask, time.Now().Add(1*time.Second))
	return t
}

func (t *RepeatedTask) Execute() (TaskStatus, time.Time) {
	return CPUTask, time.Now().Add(1 * time.Second)
}

func (t *RepeatedTask) Cancel() {
	t.taskHandle.Cancel()
}
