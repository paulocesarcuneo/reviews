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
	input, closer, err := pipe.Users.In()
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.SummaryReviewCounter.Out()
	if err != nil {
		log.Fatal(err)
	}

	controlOut <- api.Signal{Action: "Join", Name: "reviewsCounter"}
	db := make(map[string]int)
	for {
		select {
		case bulk := <-input:
			for _, user := range bulk {
				count := db[user]
				db[user] = count + 1
			}
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				summary <- utils.Copy(db)
			case api.Quit:
				return
			}
		}
	}
}
