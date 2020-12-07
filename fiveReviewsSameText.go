package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
)

func merge5ReviewsSameText(sameTextCounter, fivePlusReviews map[string]int) []string {
	result := []string{}
	for sameTextUser, _ := range sameTextCounter {
		if _, ok := fivePlusReviews[sameTextUser]; ok {
			result = append(result, sameTextUser)
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
	end := 2
loop:
	for {
		select {
		case bulk, ok := <-reviewCounter:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			for user, count := range bulk {
				if count > 5 {
					fivePlusReviews[user] = count
				}
			}
			if len(bulk) == 0 {
				end--
			}
			summary <- merge5ReviewsSameText(sameTextCounter, fivePlusReviews)
			if end == 0 {
				break loop
			}
		case bulk, ok := <-sameText:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			if len(bulk) != 0 {
				sameTextCounter = bulk
			} else {
				end--
			}
			summary <- merge5ReviewsSameText(sameTextCounter, fivePlusReviews)
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
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "fiveReviewsSameText"}
			}
		}
	}
	summary <- []string{}
	log.Println("EOF")
}
