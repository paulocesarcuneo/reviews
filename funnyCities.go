package main

import (
	"encoding/json"
	"log"
	"reviews/api"
	"reviews/pipe"
	"strings"
)

func ParseBusiness(data string) []api.Business {
	result := []api.Business{}
	for _, jsonData := range strings.Split(data, "\n") {
		if jsonData == "" {
			continue
		}
		var r api.Business
		err := json.Unmarshal([]byte(jsonData), &r)
		if err != nil {
			log.Println(jsonData, err)
			continue
		}
		result = append(result, r)
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
	business, bcloser, err := pipe.BusinessSource.In()
	if err != nil {
		log.Fatal(err)
	}
	defer bcloser.Close()
	businessText, btcloser, err := pipe.BusinessText.In()
	if err != nil {
		log.Fatal(err)
	}
	defer btcloser.Close()
	summary, err := pipe.SummaryFunnyCities.Out()
	if err != nil {
		log.Fatal(err)
	}

	businessCitiesDB := make(map[string]string)
	funnyBusinessDB := make(map[string]int)
	for {
		select {
		case bulk := <-business:
			for _, business := range ParseBusiness(bulk) {
				businessCitiesDB[business.Business_id] = business.City
			}
		case bulk := <-businessText:
			for _, business := range bulk {
				if strings.Contains(business.Text, "funny") {
					count := funnyBusinessDB[business.Business]
					funnyBusinessDB[business.Business] = count + 1
				}
			}
		case signal := <-controlIn:
			switch signal {
			case api.Emit:
				result := make(map[string]int)
				for business, count := range funnyBusinessDB {
					if city, ok := businessCitiesDB[business]; ok {
						result[city] = count + result[city]
					}
				}
				summary <- result
			case api.Quit:
				return
			case api.WakeUp:
				controlOut <- api.Signal{Action: "Join", Name: "funnyCities"}
			}
		}
	}
}
