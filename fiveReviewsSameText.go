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

	reviewCounter, bcloser, err := pipe.SummaryReviewCounter.In()
	if err != nil {
		log.Fatal(err)
	}
	defer bcloser.Close()
	sameText, btcloser, err := pipe.SummarySameText.In()
	if err != nil {
		log.Fatal(err)
	}
	defer btcloser.Close()
	summary, err := pipe.Summary5ReviewsSameText.Out()
	if err != nil {
		log.Fatal(err)
	}

	fivePlusReviews := make(map[string]int)
	sameTextCounter := make(map[string]int)
	for {
		select {
		case bulk := <-reviewCounter:
			log.Println("updating review count")
			for user, count := range bulk {
				if count > 5 {
					fivePlusReviews[user] = count
				}
			}
		case bulk := <-sameText:
			log.Println("updating same text")
			sameTextCounter = bulk
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				result := []string{}
				for sameTextUser, _ := range sameTextCounter {
					if _, ok := fivePlusReviews[sameTextUser]; ok {
						result = append(result, sameTextUser)
					}
				}
				summary <- result
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "fiveReviewsSameText"}
			}
		}
	}
}
