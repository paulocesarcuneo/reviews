package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
)

func formatSameText(db map[string]map[string]int) map[string]int {
	result := make(map[string]int)
	for user, counter := range db {
		if len(counter) != 1 {
			continue
		}
		for _, count := range counter {
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
	input, iCloser, err := pipe.UserText.In()
	if err != nil {
		log.Fatal(err)
	}
	defer iCloser.Close()
	summary, err := pipe.SummarySameText.Out()
	if err != nil {
		log.Fatal(err)
	}
	db := make(map[string]map[string]int)
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
					counter = make(map[string]int)
				}
				count := counter[user.Text]
				counter[user.Text] = count + 1
				db[user.User] = counter
			}
			if (eventCounter%api.SUMMARY_BULK_SIZE == 0 && len(db) != 0) || (len(bulk) == 0) {
				log.Println(eventCounter)
				summary <- formatSameText(db)
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
				controlOut <- api.Signal{Action: "Join", Name: "sameText"}
			}
		}
	}
	summary <- map[string]int{}
	log.Println("EOF")
}
