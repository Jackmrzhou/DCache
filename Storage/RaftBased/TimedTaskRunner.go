package RaftBased

import "time"

type TimedTaskRunner struct {
	quit chan bool
}

func (r *TimedTaskRunner) SubmitTask(interval time.Duration, task func()) error {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				task()
			case <-r.quit:
				ticker.Stop()
				return
			}
		}
	}()
	return nil
}

func (r *TimedTaskRunner) Stop() error {
	r.quit <- true
	return nil
}