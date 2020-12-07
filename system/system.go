package system

import (
	"log"
	"reviews/api"
	"reviews/pipe"
	"sync"
	"time"
)

type Services struct {
	sync.Mutex
	Services   map[string]int
	controlIn  <-chan api.Signal
	controlOut chan<- api.Signal
	Close      func() error
	State      string
}

const Initializing = "initializing"
const BusinessReady = "business-ready"
const ReviewsReady = "reviews-ready"

var KnownServices = []string{"parser",
	"reviewsCounter",
	"fiftyPlusReviews",
	"fiveReviewsSameText",
	"fiftyReviewsFiveStars",
	"histogram",
	"stars5",
	"funnyCities",
	"top10FunniestCities",
	"sameText"}

/*

init -> business-ready -> review ready -> done

*/
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
		Services:   make(map[string]int),
		controlIn:  controlIn,
		controlOut: controlOut,
		Close: func() error {
			return cCloser.Close()
		},
		State: Initializing,
	}, nil
}

func (s *Services) servicesReady() bool {
	for _, service := range KnownServices {
		if count, ok := s.Services[service]; !ok || count <= 0 {
			return false
		}
	}
	return true
}

func (s *Services) inc(name string) {
	s.Lock()
	defer s.Unlock()
	s.Services[name] = s.Services[name] + 1
	if s.servicesReady() {
		if s.State == Initializing {
			s.State = BusinessReady
		}
	}
}

func (s *Services) Start(wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		for s.State == Initializing {
			s.controlOut <- api.WakeUp
			time.Sleep(time.Duration(2) * time.Second)
		}
		log.Println("Initialization Done")
	}()
}

func (s *Services) Listen(wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
	loop:
		for signal := range s.controlIn {
			switch signal {
			case api.Quit:
				break loop
			case api.BusinessEOF:
				log.Println("Accepting Reviews Now")
				s.State = ReviewsReady
			default:
				switch signal.Action {
				case "Join":
					s.inc(signal.Name)
				}
			}
			log.Println(signal)
		}
	}()
}

func (s *Services) Send(signal api.Signal) {
	s.controlOut <- signal
}
