package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"reviews/api"
	"reviews/pipe"
	"reviews/utils"
)

func uuid() string {
	u := make([]byte, 16)
	_, err := rand.Read(u)
	if err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(u)
}
func main() {
	Id := uuid()
	controlIn, cCloser, err := pipe.Control.In()
	if err != nil {
		log.Fatal(err)
	}
	defer cCloser.Close()
	controlOut, err := pipe.Control.Out()
	if err != nil {
		log.Fatal(err)
	}
	input, closer, err := pipe.Users.In(Id)
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.SummaryReviewCounter.Out()
	if err != nil {
		log.Fatal(err)
	}

	db := make(map[string]int)
	eventCounter := 0
	controlOut <- api.Signal{Action: "Join", Name: "reviewsCounter", Id: Id}
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
				count := db[user]
				db[user] = count + 1
			}
			if (eventCounter%api.SUMMARY_BULK_SIZE == 0 && len(db) != 0) || (len(bulk) == 0) {
				log.Println(eventCounter)
				summary <- utils.Copy(db)
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
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "reviewsCounter", Id: Id}
			}
		}
	}
	summary <- map[string]int{}
	log.Println("EOF")
}
