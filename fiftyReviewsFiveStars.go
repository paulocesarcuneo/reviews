package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
)

func main() {
	controlIn, cCloser, err := pipe.Control.In()
	if err != nil {
		log.Fatal(err)
	}
	defer cCloser.Close()
	controlOut, err := pipe.Control.Out()
	if err != nil {
		log.Fatal(err)
	}
	fiftyCounter, bcloser, err := pipe.Summary50Reviews.In()
	if err != nil {
		log.Fatal(err)
	}
	defer bcloser.Close()
	fiveStars, btcloser, err := pipe.SummaryFiveStars.In()
	if err != nil {
		log.Fatal(err)
	}
	defer btcloser.Close()
	summary, err := pipe.Summary50Reviews5Stars.Out()
	if err != nil {
		log.Fatal(err)
	}

	fiftyPlusReviews := make(map[string]int)
	fiveStarsCounter := make(map[string]int)
	for {
		select {
		case bulk := <-fiftyCounter:
			log.Println("update fifty reviews count")
			fiftyPlusReviews = bulk
		case bulk := <-fiveStars:
			log.Println("update five stars count")
			fiveStarsCounter = bulk
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				result := []string{}
				for user, _ := range fiveStarsCounter {
					if _, ok := fiftyPlusReviews[user]; ok {
						result = append(result, user)
					}
				}
				summary <- result
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "fiftyReviewsFiveStars"}
			}
		}
	}
}
