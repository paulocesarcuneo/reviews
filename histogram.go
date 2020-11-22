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
	controlOut <- api.Signal{Action: "Join", Name: "histogram"}
	db := make(map[string]int)
	for {
		select {
		case bulk := <-input:
			for _, date := range bulk {
				day := date.Weekday().String()
				count := db[day]
				db[day] = count + 1
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
