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
func updateCities(funnyBusinessDB map[string]int, businessCitiesDB map[string]string) map[string]int {
	result := make(map[string]int)
	for business, count := range funnyBusinessDB {
		if city, ok := businessCitiesDB[business]; ok {
			result[city] = count + result[city]
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
	end := 2
	eventCounter := 0
loop:
	for {
		select {
		case bulk, ok := <-business:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			businessBulk := ParseBusiness(bulk)
			if len(bulk) == 0 {
				log.Println("Done Loading Business")
				controlOut <- api.BusinessEOF
				end--
			} else {
				for _, business := range businessBulk {
					businessCitiesDB[business.Business_id] = business.City
				}
			}
		case bulk, ok := <-businessText:
			if !ok {
				log.Println("Broken chan")
				break loop
			}
			eventCounter++
			for _, business := range bulk {
				if strings.Contains(business.Text, "funny") {
					count := funnyBusinessDB[business.Business]
					funnyBusinessDB[business.Business] = count + 1
				}
			}
			if len(bulk) == 0 {
				end--
			}
			if (eventCounter%api.SUMMARY_BULK_SIZE == 0 && len(funnyBusinessDB) != 0) || end == 0 {
				log.Println(eventCounter)
				summary <- updateCities(funnyBusinessDB, businessCitiesDB)
			}
			if end == 0 {
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
				controlOut <- api.Signal{Action: "Join", Name: "funnyCities"}
			}
		}
	}
	summary <- map[string]int{}
	log.Println("EOF")
}
