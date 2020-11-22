package pipe

import (
	"io"
	"log"
	"os"
	"reviews/api"
	"reviews/encoding"
	"reviews/utils"
	"time"

	"github.com/streadway/amqp"
)

var Conn *amqp.Connection

func Consume(
	ch *amqp.Channel,
	exchangeName string,
	queueName string,
	routingKey string) (<-chan amqp.Delivery, error) {
	err := ch.ExchangeDeclare(
		exchangeName, // name string
		"direct",     // kind string
		false,        // durable bool
		false,        // autoDelete bool
		false,        // internal bool
		true,         // noWait bool
		nil,          // args amqp.Table
	)
	if err != nil {
		return nil, err
	}
	queue, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}
	err = ch.QueueBind(queue.Name, routingKey, exchangeName, false, nil)
	if err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	return msgs, err
}

type BytesConsumer func([]byte) error

func ConsumeWith(exchange, queue, routingKey string,
	consumer BytesConsumer) (*amqp.Channel, error) {
	ch, err := Conn.Channel()
	if err != nil {
		return nil, err
	}
	delivery, err := Consume(ch, exchange, queue, routingKey)
	if err != nil {
		return nil, err
	}
	go func() {
		for d := range delivery {
			consumer(d.Body)
		}
	}()
	return ch, nil
}

type Consumer func(interface{}) error

func ConsumeAsGob(exchange,
	queue,
	routingKey string,
	consumer Consumer) (*amqp.Channel, error) {
	return ConsumeWith(exchange,
		queue,
		routingKey,
		func(bytes []byte) error {
			data, err := encoding.GobDecoder(bytes)
			if err != nil {
				return err
			}
			return consumer(data)
		})
}

type Worker func(consumer BytesConsumer)

func Publish(threads int,
	exchangeName string,
	routingKey string,
	worker Worker) error {
	utils.Pool(threads, func() {
		ch, err := Conn.Channel()
		if err != nil {
			log.Println("Publish", err)
		}
		err = ch.ExchangeDeclare(
			exchangeName, // name string
			"direct",     // kind string
			false,        // durable bool
			false,        // autoDelete bool
			false,        // internal bool
			true,         // noWait bool
			nil,          // args amqp.Table
		)
		if err != nil {
			log.Println("Publish", err)
		}
		worker(func(bytes []byte) error {
			return ch.Publish(
				exchangeName, // exchange
				routingKey,   // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType: "application/octet-stream",
					Body:        bytes,
				})
		})
		ch.Close()
	})
	return nil
}

type GobWorker func(Consumer)

func PublishGob(
	threads int,
	exchangeName string,
	routingKey string,
	worker GobWorker) error {
	return Publish(threads,
		exchangeName,
		routingKey,
		func(byteconsumer BytesConsumer) {
			worker(func(data interface{}) error {
				bytes, err := encoding.GobEncoder(data)
				if err != nil {
					log.Fatal(err)
				}
				return byteconsumer(bytes)
			})
		})
}

type StringPipe struct {
	In  func() (<-chan string, io.Closer, error)
	Out func() (chan<- string, error)
}

func NewStringPipe(exchange, queueName string) StringPipe {
	return StringPipe{
		In: func() (<-chan string, io.Closer, error) {
			data := make(chan string)
			ch, err := ConsumeWith(exchange,
				queueName,
				exchange,
				func(bytes []byte) error {
					data <- string(bytes)
					return nil
				})

			return data, ch, err
		},
		Out: func() (chan<- string, error) {
			data := make(chan string)
			err := Publish(2,
				exchange,
				exchange,
				func(consumer BytesConsumer) {
					for s := range data {
						consumer([]byte(s))
					}
				})
			return data, err
		},
	}
}

type StringArrayPipe struct {
	In  func() (<-chan []string, io.Closer, error)
	Out func() (chan<- []string, error)
}

func NewStringArrayPipe(exchange, queue string) StringArrayPipe {
	return StringArrayPipe{
		In: func() (<-chan []string, io.Closer, error) {
			data := make(chan []string)
			ch, err := ConsumeAsGob(exchange,
				queue,
				exchange,
				func(t interface{}) error {
					data <- t.([]string)
					return nil
				})
			return data, ch, err
		},
		Out: func() (chan<- []string, error) {
			data := make(chan []string)
			err := PublishGob(2,
				exchange,
				exchange,
				func(consumer Consumer) {
					for i := range data {
						consumer(i)
					}
				})
			return data, err
		},
	}
}

// Public Pipes
var ReviewSource = NewStringPipe("reviews", "reviews")
var BusinessSource = NewStringPipe("business", "business")
var Users = NewStringArrayPipe("review-users", "review-users")

const ReviewDates = "review-dates"

var Dates = struct {
	In  func() (<-chan []time.Time, io.Closer, error)
	Out func() (chan<- []time.Time, error)
}{
	In: func() (<-chan []time.Time, io.Closer, error) {
		data := make(chan []time.Time)
		ch, err := ConsumeAsGob(ReviewDates,
			ReviewDates,
			ReviewDates,
			func(t interface{}) error {
				data <- t.([]time.Time)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- []time.Time, error) {
		data := make(chan []time.Time)
		err := PublishGob(2,
			ReviewDates,
			ReviewDates,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

const ReviewUserStars = "review-user-stars"

var UserStars = struct {
	In  func() (<-chan []api.UserStars, io.Closer, error)
	Out func() (chan<- []api.UserStars, error)
}{
	In: func() (<-chan []api.UserStars, io.Closer, error) {
		data := make(chan []api.UserStars)
		ch, err := ConsumeAsGob(ReviewUserStars,
			ReviewUserStars,
			ReviewUserStars,
			func(t interface{}) error {
				data <- t.([]api.UserStars)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- []api.UserStars, error) {
		data := make(chan []api.UserStars)
		err := PublishGob(2,
			ReviewUserStars,
			ReviewUserStars,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

const ReviewBusinessTexts = "review-business-texts"

var BusinessText = struct {
	In  func() (<-chan []api.BusinessText, io.Closer, error)
	Out func() (chan<- []api.BusinessText, error)
}{
	In: func() (<-chan []api.BusinessText, io.Closer, error) {
		data := make(chan []api.BusinessText)
		ch, err := ConsumeAsGob(ReviewBusinessTexts,
			ReviewBusinessTexts,
			ReviewBusinessTexts,
			func(t interface{}) error {
				data <- t.([]api.BusinessText)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- []api.BusinessText, error) {
		data := make(chan []api.BusinessText)
		err := PublishGob(2,
			ReviewBusinessTexts,
			ReviewBusinessTexts,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

const ReviewUserTexts = "review-user-texts"

var UserText = struct {
	In  func() (<-chan []api.UserText, io.Closer, error)
	Out func() (chan<- []api.UserText, error)
}{
	In: func() (<-chan []api.UserText, io.Closer, error) {
		data := make(chan []api.UserText)
		ch, err := ConsumeAsGob(ReviewUserTexts,
			ReviewUserTexts,
			ReviewUserTexts,
			func(t interface{}) error {
				data <- t.([]api.UserText)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- []api.UserText, error) {
		data := make(chan []api.UserText)
		err := PublishGob(2,
			ReviewUserTexts,
			ReviewUserTexts,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

type CounterPipe struct {
	In  func() (<-chan map[string]int, io.Closer, error)
	Out func() (chan<- map[string]int, error)
}

func NewCounterPipe(name string, queueName string) CounterPipe {
	return CounterPipe{
		In: func() (<-chan map[string]int, io.Closer, error) {
			data := make(chan map[string]int)
			ch, err := ConsumeAsGob(name,
				queueName,
				name,
				func(t interface{}) error {
					data <- t.(map[string]int)
					return nil
				})
			return data, ch, err
		},
		Out: func() (chan<- map[string]int, error) {
			data := make(chan map[string]int)
			err := PublishGob(2,
				name,
				name,
				func(consumer Consumer) {
					for i := range data {
						consumer(i)
					}
				})
			return data, err
		},
	}
}

var SummaryFunnyCities = NewCounterPipe("summary-funny-cities", "summary-funny-cities")
var SummaryHistogram = NewCounterPipe("summary-histogram", "summary-histogram")
var SummaryReviewCounter = NewCounterPipe("summary-review-counter", "")
var SummaryFiveStars = NewCounterPipe("summary-five-stars", "summary-five-stars")
var SummarySameText = NewCounterPipe("summary-same-text", "summary-same-text")

const Top10FunnyCities = "summary-top10-funny-cities"

var SummaryTop10FunnyCities = struct {
	In  func() (<-chan []api.StringInt, io.Closer, error)
	Out func() (chan<- []api.StringInt, error)
}{
	In: func() (<-chan []api.StringInt, io.Closer, error) {
		data := make(chan []api.StringInt)
		ch, err := ConsumeAsGob(Top10FunnyCities,
			Top10FunnyCities,
			Top10FunnyCities,
			func(t interface{}) error {
				data <- t.([]api.StringInt)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- []api.StringInt, error) {
		data := make(chan []api.StringInt)
		err := PublishGob(2,
			Top10FunnyCities,
			Top10FunnyCities,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

var Summary50Reviews = NewCounterPipe("summary-50-reviews", "summary-50-reviews")
var Summary5ReviewsSameText = NewStringArrayPipe("summary-5-reviews-same-text", "summary-5-reviews-same-text")
var Summary50Reviews5Stars = NewStringArrayPipe("summary-50-reviews-5-stars", "summary-50-reviews-5-stars")

const control = "contol"

type ControlPipe struct {
	In  func() (<-chan api.Signal, io.Closer, error)
	Out func() (chan<- api.Signal, error)
}

var Control = ControlPipe{
	In: func() (<-chan api.Signal, io.Closer, error) {
		data := make(chan api.Signal)
		ch, err := ConsumeAsGob(control,
			"",
			control,
			func(t interface{}) error {
				data <- t.(api.Signal)
				return nil
			})
		return data, ch, err
	},
	Out: func() (chan<- api.Signal, error) {
		data := make(chan api.Signal)
		err := PublishGob(2,
			control,
			control,
			func(consumer Consumer) {
				for i := range data {
					consumer(i)
				}
			})
		return data, err
	},
}

func rabbitUrl() string {
	url := os.Getenv("RABBIT_URL")
	if url == "" {
		return "amqp://guest:guest@localhost:5672/"
	}
	return url
}

func init() {
	url := rabbitUrl()
	aux, err := utils.Retry(8,
		func() (interface{}, error) {
			return amqp.Dial(url)
		})
	if err != nil {
		log.Fatal(err)
	}
	Conn = aux.(*amqp.Connection)
}
