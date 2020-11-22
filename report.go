package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reviews/api"
	"reviews/pipe"
	"sync"
)

type Report struct {
	Histogram          map[string]int
	Top10Funniest      []api.StringInt
	Reviews50Plus      map[string]int
	Reviews50FiveStars []string
	Reviews5SameText   []string
}

func ReportWorker(report *Report, wg *sync.WaitGroup) func() error {
	fifty50Stars5, fsCloser, err := pipe.Summary50Reviews5Stars.In()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for stars := range fifty50Stars5 {
			report.Reviews50FiveStars = stars
		}
	}()

	fiveSameText, ftCloser, err := pipe.Summary5ReviewsSameText.In()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		wg.Add(1)
		defer wg.Done()
		for same := range fiveSameText {
			report.Reviews5SameText = same
		}
	}()

	fiftyPlus, fpCloser, err := pipe.Summary50Reviews.In()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for fifty := range fiftyPlus {
			report.Reviews50Plus = fifty
		}
	}()

	histogram, hCloser, err := pipe.SummaryHistogram.In()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for histo := range histogram {
			report.Histogram = histo
		}
	}()

	top10Cities, tcCloser, err := pipe.SummaryTop10FunnyCities.In()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for city := range top10Cities {
			report.Top10Funniest = city
		}
	}()

	return func() error {
		tcCloser.Close()
		hCloser.Close()
		fpCloser.Close()
		fsCloser.Close()
		ftCloser.Close()
		return nil
	}
}

func main() {
	var wg sync.WaitGroup
	report := Report{
		Histogram:          make(map[string]int),
		Reviews50FiveStars: []string{},
		Reviews5SameText:   []string{},
		Reviews50Plus:      make(map[string]int),
		Top10Funniest:      []api.StringInt{},
	}
	closer := ReportWorker(&report, &wg)
	control, err := pipe.Control.Out()
	if err != nil {
		log.Fatal(err)
	}

	reviews, err := pipe.ReviewSource.Out()
	if err != nil {
		log.Fatal(err)
	}
	business, err := pipe.BusinessSource.Out()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		wg.Add(1)
		defer wg.Done()
		pipe.RegisterAndWait("parser", closer, func() {})
	}()

	http.HandleFunc("/business", func(res http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
		} else {
			business <- string(data)
		}
		fmt.Fprintln(res, "")
	})

	http.HandleFunc("/reviews", func(res http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Println(err)
		} else {
			reviews <- string(data)
		}
		fmt.Fprintln(res, "")
	})

	http.HandleFunc("/report", func(res http.ResponseWriter, req *http.Request) {
		control <- api.Emit
		res.Header().Add("Content-Type", "application/json")
		encoder := json.NewEncoder(res)
		encoder.SetIndent("", "\t")
		err := encoder.Encode(report)
		log.Println(err)
	})

	http.HandleFunc("/quit", func(res http.ResponseWriter, req *http.Request) {
		control <- api.Quit
		fmt.Fprintln(res, "")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))

	wg.Wait()
}
