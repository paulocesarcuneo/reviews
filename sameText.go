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

	for {
		select {
		case bulk := <-input:
			for _, user := range bulk {
				counter := db[user.User]
				if counter == nil {
					counter = make(map[string]int)
				}
				count := counter[user.Text]
				counter[user.Text] = count + 1
				db[user.User] = counter
			} /**/
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				result := make(map[string]int)
				for user, counter := range db {
					if len(counter) != 1 {
						continue
					}
					for _, count := range counter {
						result[user] = count
					}
				}
				summary <- result
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "sameText"}
			}
		}
		/**/
	}
}
