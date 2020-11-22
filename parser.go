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
	log.Println("ok")
	var wg sync.WaitGroup
	threads := 4
	wg.Add(threads)
	utils.Pool(threads, func() {
		defer wg.Done()
		defer rClose.Close()
		defer ccloser.Close()
		for {
			select {
			case bulk := <-reviews:
				reviews := ParseReview(bulk)
				users <- api.MapUsers(reviews)
				dates <- api.MapDate(reviews)
				stars <- api.MapUserStars(reviews)
				text <- api.MapUserText(reviews)
				business <- api.MapBusinessText(reviews)
			case signal := <-controlIn:
				switch signal {
				case api.Quit:
					return
				case api.WakeUp:
					controlOut <- api.Signal{Action: "Join", Name: "parser"}
				}
			}
		}
	})
	wg.Wait()
}
