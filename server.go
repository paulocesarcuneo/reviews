package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reviews/api"
	"reviews/pipe"
	"reviews/report"
	"reviews/system"
	"sync"
)

func main() {
	reviews, err := pipe.ReviewSource.Out()
	if err != nil {
		log.Fatal(err)
	}
	business, err := pipe.BusinessSource.Out()
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	report, rCloser := report.LaunchReportWorker(&wg)
	defer rCloser()

	services := &system.Services{Services: make(map[string]bool)}
	go func() {
		wg.Add(1)
		defer wg.Done()
		services.Start()
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
		if !services.Enabled() {
			services.Send(api.WakeUp)
			fmt.Fprintln(res, "Services not enabled")
			return
		}
		services.Send(api.Emit)
		res.Header().Add("Content-Type", "application/json")
		encoder := json.NewEncoder(res)
		encoder.SetIndent("", "\t")
		err := encoder.Encode(report)
		log.Println(err)
	})

	http.HandleFunc("/control", func(res http.ResponseWriter, req *http.Request) {
		decoder := json.NewDecoder(req.Body)
		var signal api.Signal
		decoder.Decode(&signal)
		services.Send(signal)
		fmt.Fprintln(res, "")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
	wg.Wait()
}
