package main

import (
	"log"
	"reviews/api"
	"reviews/pipe"
	"sort"
)

func top10(cities map[string]int) []api.StringInt {
	result := []api.StringInt{}
	for key, val := range cities {
		result = append(result, api.StringInt{String: key, Int: val})
	}
	sort.Sort(api.ByCounter(result))
	if len(result) > 10 {
		return result[:10]
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
	input, closer, err := pipe.SummaryFunnyCities.In()
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()
	summary, err := pipe.SummaryTop10FunnyCities.Out()
	if err != nil {
		log.Fatal(err)
	}
	db := []api.StringInt{}
	for {
		select {
		case bulk := <-input:
			db = top10(bulk)
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				summary <- db
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "top10FunniestCities"}
			}
		}
	}
}
