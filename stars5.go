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
	input, closer, err := pipe.UserStars.In()
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.SummaryFiveStars.Out()
	if err != nil {
		log.Fatal(err)
	}

	db := make(map[string]map[float64]int)
	for {
		select {
		case bulk := <-input:
			for _, user := range bulk {
				counter := db[user.User]
				if counter == nil {
					counter = make(map[float64]int)
				}
				count := counter[user.Stars]
				counter[user.Stars] = count + 1
				db[user.User] = counter
			}
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				result := make(map[string]int)
				for user, counter := range db {
					if len(counter) != 1 {
						continue
					}
					if count, ok := counter[api.Five]; ok {
						result[user] = count
					}
				}
				summary <- result
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "stars5"}
			}
		}
	}
}
