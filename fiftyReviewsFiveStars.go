package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
)

func merge5Stars50Reviews(fiveStarsCounter, fiftyPlusReviews map[string]int) []string {
	result := []string{}
	for user, _ := range fiveStarsCounter {
		if _, ok := fiftyPlusReviews[user]; ok {
			result = append(result, user)
		}
	}
	return result
}
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
	end := 2
loop:
	for {
		select {
		case bulk, ok := <-fiftyCounter:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			if len(bulk) != 0 {
				fiftyPlusReviews = bulk
			} else {
				log.Println("End of fifty counter")
				end--
			}
			summary <- merge5Stars50Reviews(fiveStarsCounter, fiftyPlusReviews)
			if end == 0 {
				break loop
			}
		case bulk, ok := <-fiveStars:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			if len(bulk) != 0 {
				fiveStarsCounter = bulk
			} else {
				log.Println("End of five stars")
				end--
			}
			summary <- merge5Stars50Reviews(fiveStarsCounter, fiftyPlusReviews)
			if end == 0 {
				break loop
			}
		case signal, ok := <-controlIn:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			switch signal {
			case api.Quit:
				break loop
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "fiftyReviewsFiveStars"}
			}
		}
	}
	summary <- []string{}
	log.Println("EOF")
}
