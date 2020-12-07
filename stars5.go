package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
)

func format5Stars(db map[string]map[float64]int) map[string]int {
	result := make(map[string]int)
	for user, counter := range db {
		if len(counter) != 1 {
			continue
		}
		if count, ok := counter[api.Five]; ok {
			result[user] = count
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
	eventCounter := 0
loop:
	for {
		select {
		case bulk, ok := <-input:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			eventCounter++
			for _, user := range bulk {
				counter := db[user.User]
				if counter == nil {
					counter = make(map[float64]int)
				}
				count := counter[user.Stars]
				counter[user.Stars] = count + 1
				db[user.User] = counter
			}
			if (eventCounter%api.SUMMARY_BULK_SIZE == 0 && len(db) != 0) || (len(bulk) == 0) {
				log.Println(eventCounter)
				fiveStars := format5Stars(db)
				summary <- fiveStars
			}
			if len(bulk) == 0 {
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
				controlOut <- api.Signal{Action: "Join", Name: "stars5"}
			}
		}
	}
	summary <- map[string]int{}
	log.Println("EOF")
}
