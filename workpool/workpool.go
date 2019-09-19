package workpool

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/springwiz/loggerdaemon/common"
	"go.uber.org/atomic"
)

// Config for Workpool
type Workpool struct {
	// Task workers
	Tasks chan Worker

	// Waitgroup
	Wg sync.WaitGroup

	// Error handling
	Hold *atomic.Bool
}

type Worker interface {
	DoWork(id int, publishMap map[string]common.Publisher, seqNumber uint64) error
}

var PoolWorkers *Workpool

func NewWorkpool(maxGoRoutines int) *Workpool {
	w := Workpool{
		Tasks: make(chan Worker, 1000),
		Hold:  atomic.NewBool(false),
	}
	w.Wg.Add(maxGoRoutines)
	for i := 1; i <= maxGoRoutines; i++ {
		go func(indx int) {
			log.Println("worker", indx, "started")
			publishMap := make(map[string]common.Publisher)
			var seqNumber uint64 = 1
			for t := range w.Tasks {
			wait:
				if w.Hold.Load() {
					time.Sleep(2 * 60 * 1000000000)
					w.Hold.Store(false)
				}
				err := t.DoWork(indx, publishMap, seqNumber)
				if err != nil {
					// sleep before retrying
					w.Hold.Store(true)
					seqNumber = 1
					goto wait
				} else {
					seqNumber++
				}
			}
			defer w.Wg.Done()
			defer w.cleanupPublisher(publishMap)
		}(i)
	}
	return &w
}

// pause workpool
func (w *Workpool) Pause() {
	w.Hold.Store(true)
}

// resume workpool
func (w *Workpool) Resume() {
	w.Hold.Store(false)
}

// check if workpool paused?
func (w *Workpool) IsPaused() bool {
	return w.Hold.Load()
}

// addTask submits work to the pool.
func (w *Workpool) AddTask(worker Worker) {
	w.Tasks <- worker
}

// Shutdown waits for all the goroutines to shutdown.
func (w *Workpool) Shutdown() {
	close(w.Tasks)
	w.Wg.Wait()
}

// clean up the publishers
func (w *Workpool) cleanupPublisher(publishMap map[string]common.Publisher) {
	for _, v := range publishMap {
		v.Cleanup()
	}
}
