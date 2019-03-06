package common

import (
	"fmt"
	"sync"
	"time"
)

type TimeOutLog struct {
	logs      []string
	started   bool
	startTime time.Time
	mu        sync.Mutex
	stopCh    chan struct{}
	tag       string
}

func NewTimeOutLog(tag string) *TimeOutLog {
	return &TimeOutLog{
		tag: tag,
	}
}
func (tol *TimeOutLog) Log(msg string) {
	tol.mu.Lock()
	defer tol.mu.Unlock()
	if tol.started == false {
		return
	}
	tol.logs = append(tol.logs, fmt.Sprintf("[%v] %s", time.Since(tol.startTime), msg))
}

func (tol *TimeOutLog) outputLogs() {
	tol.mu.Lock()
	if tol.started == false {
		return
	}
	tmp := tol.logs
	tol.started = false
	close(tol.stopCh)
	tol.logs = []string{}
	tol.mu.Unlock()
	for i, msg := range tmp {
		fmt.Printf("timeoutlog [%s] [%d] %s\n", tol.tag, i, msg)
	}
}

func (tol *TimeOutLog) Start(timeOut time.Duration, bufLen int) {
	tol.mu.Lock()
	defer tol.mu.Unlock()
	tol.started = true
	tol.startTime = time.Now()
	tol.stopCh = make(chan struct{})
	tol.logs = make([]string, 0, bufLen)
	go func() {
		select {
		case <-time.After(timeOut):
			tol.outputLogs()
		case <-tol.stopCh:
		}
	}()
}

func (tol *TimeOutLog) Stop() {
	tol.mu.Lock()
	defer tol.mu.Unlock()
	if tol.started == true {
		tol.started = false
		tol.logs = []string{}
		close(tol.stopCh)
	}
}
