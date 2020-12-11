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

	services, err := system.NewService()
	services.Start(&wg)
	services.Listen(&wg)

	http.HandleFunc("/business", func(res http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(res, err.Error())
		} else {
			business <- string(data)
			fmt.Fprintln(res, "")
		}
	})

	http.HandleFunc("/reviews", func(res http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			res.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(res, err.Error())
		} else {
			reviews <- string(data)
			fmt.Fprintln(res, "")
		}
	})

	http.HandleFunc("/report", func(res http.ResponseWriter, req *http.Request) {
		if services.State == system.Initializing {
			res.WriteHeader(http.StatusPreconditionFailed)
			fmt.Fprintln(res, "Can't process reviews server state is ", services.State)
			return
		}
		res.Header().Add("Content-Type", "application/json")
		encoder := json.NewEncoder(res)
		encoder.SetIndent("", "\t")
		err := encoder.Encode(report)
		if err != nil {
			fmt.Fprintf(res, err.Error())
		} else {
			fmt.Fprintf(res, "")
		}
	})

	http.HandleFunc("/control", func(res http.ResponseWriter, req *http.Request) {
		decoder := json.NewDecoder(req.Body)
		var signal api.Signal
		decoder.Decode(&signal)
		services.Send(signal)
		fmt.Fprintln(res, "")
	})
	log.Println("Starting server")
	log.Fatal(http.ListenAndServe(":8080", nil))
	wg.Wait()
}
