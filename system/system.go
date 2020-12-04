package system

import (
	"log"
	"reviews/api"
	"reviews/pipe"
	"sync"
)

type Services struct {
	sync.Mutex
	Services   map[string]bool
	controlIn  <-chan api.Signal
	controlOut chan<- api.Signal
	Close      func() error
}

func NewService() (*Services, error) {
	controlIn, cCloser, err := pipe.Control.In()
	if err != nil {
		return nil, err
	}
	controlOut, err := pipe.Control.Out()
	if err != nil {
		return nil, err
	}
	return &Services{
		Services:   make(map[string]bool),
		controlIn:  controlIn,
		controlOut: controlOut,
		Close: func() error {
			return cCloser.Close()
		},
	}, nil
}

func (s *Services) Enabled() bool {
	s.Lock()
	defer s.Unlock()
	for _, service := range []string{"parser",
		"reviewsCounter",
		"fiftyPlusReviews",
		"fiveReviewsSameText",
		"fiftyReviewsFiveStars",
		"histogram",
		"stars5",
		"funnyCities",
		"top10FunniestCities",
		"sameText"} {
		if enabled, ok := s.Services[service]; !ok || !enabled {
			return false
		}
	}
	return true
}

func (s *Services) Status(name string, enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.Services[name] = enabled
}

func (s *Services) Start() {
	s.controlOut <- api.WakeUp
loop:
	for signal := range s.controlIn {
		log.Println(signal)
		switch signal.Action {
		case "Quit":
			break loop
		case "Join":
			s.Status(signal.Name, true)
		case "Leave":
			s.Status(signal.Name, false)
		}
	}
}

func (s *Services) Send(signal api.Signal) {
	s.controlOut <- signal
}
