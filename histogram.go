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
	input, closer, err := pipe.Dates.In()
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.SummaryHistogram.Out()
	if err != nil {
		log.Fatal(err)
	}
	db := make(map[string]int)
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
			for _, date := range bulk {
				day := date.Weekday().String()
				count := db[day]
				db[day] = count + 1
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
				break loop
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "histogram"}
			}
		}
	}
	summary <- map[string]int{}
	log.Println("EOF")
}
