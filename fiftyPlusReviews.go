package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
	"reviews/utils"
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
	input, closer, err := pipe.SummaryReviewCounter.In()
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.Summary50Reviews.Out()
	if err != nil {
		log.Fatal(err)
	}
	db := make(map[string]int)
loop:
	for {
		select {
		case userCount, ok := <-input:
			for user, count := range userCount {
				if count > 50 {
					db[user] = count
				}
			}
			if len(db) != 0 || len(userCount) == 0 {
				summary <- utils.Copy(db)
			}
			if len(userCount) == 0 {
				break loop
			}
			if !ok {
				log.Println("Broken chan")
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
				controlOut <- api.Signal{Action: "Join", Name: "fiftyPlusReviews"}
			}
		}
	}
	log.Println("EOF")
	summary <- map[string]int{}
}
