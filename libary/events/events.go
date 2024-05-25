package events

import (
	"sync"
	"time"

	"github.com/alimy/tryst/cfg"
	"github.com/alimy/tryst/pool"
	"github.com/longpi1/gopkg/libary/log"
	"github.com/robfig/cron/v3"
)

var (
	_defaultEventManager EventManager
	_defaultJobManager   JobManager = emptyJobManager{}
	_onceInitial         sync.Once
)

type eventManagerConf struct {
	MinWorker       int
	MaxEventBuf     int
	MaxTempEventBuf int
	MaxTickCount    int
	MaxIdeaTime     time.Duration
}

func StartEventManager() {
	_defaultEventManager.Start()
}

func StopEventManager() {
	_defaultEventManager.Stop()
}

// OnEvent push event to gorotine pool then handled automatic.
func OnEvent(event Event) {
	_defaultEventManager.OnEvent(event)
}

func StartJobManager() {
	_defaultJobManager.Start()
}

func StopJobManager() {
	_defaultJobManager.Stop()
}

// NewJob create new Job instance
func NewJob(s cron.Schedule, fn JobFn) Job {
	return &simpleJob{
		Schedule: s,
		Job:      fn,
	}
}

// RemoveJob an entry from being run in the future.
func RemoveJob(id EntryID) {
	_defaultJobManager.Remove(id)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func Schedule(job Job) EntryID {
	return _defaultJobManager.Schedule(job)
}

// OnTask adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func OnTask(s cron.Schedule, fn JobFn) EntryID {
	job := &simpleJob{
		Schedule: s,
		Job:      fn,
	}
	return _defaultJobManager.Schedule(job)
}

func Initial(conf eventManagerConf) {
	_onceInitial.Do(func() {
		initEventManager(conf)
		cfg.Not("DisableJobManager", func() {
			initJobManager()
			log.Debug("initial JobManager")
		})
	})
}

func initJobManager() {
	_defaultJobManager = NewJobManager()
	StartJobManager()
}

func initEventManager(conf eventManagerConf) {
	var opts []pool.Option
	if conf.MinWorker > 5 {
		opts = append(opts, pool.WithMinWorker(conf.MinWorker))
	} else {
		opts = append(opts, pool.WithMinWorker(5))
	}
	if conf.MaxEventBuf > 10 {
		opts = append(opts, pool.WithMaxRequestBuf(conf.MaxEventBuf))
	} else {
		opts = append(opts, pool.WithMaxRequestBuf(10))
	}
	if conf.MaxTempEventBuf > 10 {
		opts = append(opts, pool.WithMaxTempWorker(conf.MaxTempEventBuf))
	} else {
		opts = append(opts, pool.WithMaxRequestTempBuf(10))
	}
	opts = append(opts, pool.WithMaxIdelTime(conf.MaxIdeaTime))
	_defaultEventManager = NewEventManager(func(req Event, err error) {
		if err != nil {
			log.Error("handle event[%s] occurs error: %s", req.Name(), err)
		}
	}, opts...)
}
