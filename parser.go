package main

import (
	"encoding/json"
	"log"
	"reviews/api"
	"reviews/pipe"
	"reviews/utils"
	"strings"
	"sync"
)

func ParseReview(data string) []api.Review {
	result := []api.Review{}
	for _, jsonData := range strings.Split(data, "\n") {
		if jsonData == "" {
			continue
		}
		var r api.Review
		err := json.Unmarshal([]byte(jsonData), &r)
		if err != nil {
			log.Println(jsonData, err)
			continue
		}
		result = append(result, r)
	}
	return result
}

func main() {
	reviews, rClose, err := pipe.ReviewSource.In()
	if err != nil {
		log.Fatal(err)
	}

	users, err := pipe.Users.Out()
	if err != nil {
		log.Fatal(err)
	}

	dates, err := pipe.Dates.Out()
	if err != nil {
		log.Fatal(err)
	}

	stars, err := pipe.UserStars.Out()
	if err != nil {
		log.Fatal(err)
	}

	text, err := pipe.UserText.Out()
	if err != nil {
		log.Fatal(err)
	}

	business, err := pipe.BusinessText.Out()
	if err != nil {
		log.Fatal(err)
	}
	controlIn, ccloser, err := pipe.Control.In()
	if err != nil {
		log.Fatal(err)
	}
	controlOut, err := pipe.Control.Out()
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	threads := 4
	wg.Add(threads)
	done := make(chan int, 1)
	utils.Pool(threads, func() {
		defer wg.Done()
		defer rClose.Close()
		defer ccloser.Close()
		for {
			select {
			case count := <-done:
				log.Println("done")
				if count > 0 {
					done <- (count - 1)
				}
				return
			case bulk := <-reviews:
				reviews := ParseReview(bulk)
				users <- api.MapUsers(reviews)
				dates <- api.MapDate(reviews)
				stars <- api.MapUserStars(reviews)
				text <- api.MapUserText(reviews)
				business <- api.MapBusinessText(reviews)
				if len(bulk) == 0 {
					// una sola instancia va a detectar el EOF, propagarlo hacia abajo
					// y comunicandolo a las demas usando el canal de control
					// como la queue de rabbit es unica no deberia haber una race condition
					log.Println("Reviews EOF Reached")
					controlOut <- api.ReviewsEOF
				}
			case signal := <-controlIn:
				switch signal {
				case api.WakeUp:
					controlOut <- api.Signal{Action: "Join", Name: "parser"}
				case api.ReviewsEOF:
					log.Println("Done Parsing Reviews")
					done <- (threads - 1)
					return
				}
			}
		}
	})
	wg.Wait()
}
