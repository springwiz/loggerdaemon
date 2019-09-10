package workpool

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/springwiz/loggerdaemon/common"
)

// Config for Workpool
type Workpool struct {
	// Task workers
	Tasks chan Worker

	// Waitgroup
	Wg sync.WaitGroup

	// Error handling
	Hold bool
}

type Worker interface {
	DoWork(id int, publishMap map[string]common.Publisher, seqNumber uint64) error
}

var PoolWorkers *Workpool

func NewWorkpool(maxGoRoutines int) *Workpool {
	w := Workpool{
		Tasks: make(chan Worker, 1000),
		Hold:  false,
	}
	w.Wg.Add(maxGoRoutines)
	for i := 1; i <= maxGoRoutines; i++ {
		go func(indx int) {
			log.Println("worker", indx, "started")
			publishMap := make(map[string]common.Publisher)
			var seqNumber uint64
			seqNumber = 1
			for t := range w.Tasks {
				err := t.DoWork(indx, publishMap, seqNumber)
				if err != nil {
					// sleep before retrying
					w.recoverFromError()
					seqNumber = 1
					time.Sleep(2 * 60 * 1000000000)
					t.DoWork(indx, publishMap, seqNumber)
				}
				seqNumber += 1
			}
			defer w.Wg.Done()
			defer w.cleanupPublisher(publishMap)
		}(i)
	}
	return &w
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

// recover from error
func (w *Workpool) recoverFromError() {
	if r := recover(); r != nil {
		fmt.Println("recovered from ", r)
	}
	w.Hold = true
}

// clean up the publishers
func (w *Workpool) cleanupPublisher(publishMap map[string]common.Publisher) {
	for _, v := range publishMap {
		v.Cleanup()
	}
}
