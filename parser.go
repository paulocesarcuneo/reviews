package main

import (
	"encoding/json"
	"io"
	"log"
	"reviews/api"
	"reviews/pipe"
	"reviews/utils"
	"strings"
	"sync"
)

func ParseReview(data string) []api.Review {
	result := []api.Review{}
	for _, jsonData := range strings.Split(data, "\n") {
		if jsonData == "" {
			continue
		}
		var r api.Review
		err := json.Unmarshal([]byte(jsonData), &r)
		if err != nil {
			log.Println(jsonData, err)
			continue
		}
		result = append(result, r)
	}
	return result
}

func ParseWorkers(threads int, wg *sync.WaitGroup) io.Closer {
	reviews, rClose, err := pipe.ReviewSource.In()
	if err != nil {
		log.Fatal(err)
	}

	users, err := pipe.Users.Out()
	if err != nil {
		log.Fatal(err)
	}

	dates, err := pipe.Dates.Out()
	if err != nil {
		log.Fatal(err)
	}

	stars, err := pipe.UserStars.Out()
	if err != nil {
		log.Fatal(err)
	}

	text, err := pipe.UserText.Out()
	if err != nil {
		log.Fatal(err)
	}

	business, err := pipe.BusinessText.Out()
	if err != nil {
		log.Fatal(err)
	}

	utils.Pool(threads, func() {
		wg.Add(1)
		defer wg.Done()
		for bulk := range reviews {
			reviews := ParseReview(bulk)
			users <- api.MapUsers(reviews)
			dates <- api.MapDate(reviews)
			stars <- api.MapUserStars(reviews)
			text <- api.MapUserText(reviews)
			business <- api.MapBusinessText(reviews)
		}
	})
	return rClose
}

func main() {
	var wg sync.WaitGroup
	worker := ParseWorkers(4, &wg)
	pipe.RegisterAndWait("parser", worker.Close, func() {})
	wg.Wait()
}
