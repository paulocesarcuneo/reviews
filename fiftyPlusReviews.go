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
	for {
		select {
		case userCount := <-input:
			log.Println("updating")
			for user, count := range userCount {
				if count > 50 {
					db[user] = count
				}
			}
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				summary <- utils.CopyN(db, 20)
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "fiftyPlusReviews"}
			}
		}
	}

}
