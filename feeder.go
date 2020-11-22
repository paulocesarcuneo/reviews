package main

import (
	"archive/zip"
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reviews/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Service string
	Data    string
}

const HOST = "http://localhost:8080/"

func spawnFeeders(
	threads int,
	wg sync.WaitGroup,
	lines chan Request) {
	utils.Pool(threads, func() {
		wg.Add(1)
		defer wg.Done()
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
			},
			Timeout: time.Duration(3) * time.Second,
		}
		for request := range lines {
			utils.Retry(8, func() (interface{}, error) {
				req, err := http.NewRequest("POST", HOST+request.Service, strings.NewReader(request.Data))
				if err != nil {
					return nil, err
				}
				resp, err := client.Do(req)
				if err != nil {
					return nil, err
				}
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
				return nil, nil
			})
		}
	})

}

func postZipFile(filePath string, service string, linesChan chan Request, chunksize int) {
	zipFiles, err := zip.OpenReader(filePath)
	if err != nil {
		panic(err)
	}
	defer zipFiles.Close()
	for _, zipFile := range zipFiles.File {
		file, err := zipFile.Open()
		if err != nil {
			panic(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			bulk := ""
			for i := 0; i < chunksize && scanner.Scan(); i++ {
				bulk = bulk + "\n" + scanner.Text()
			}
			linesChan <- Request{Data: bulk, Service: service}
		}
		log.Println("Done Reading File", service)
	}
}

func main() {
	chunksize, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	linesChan := make(chan Request)
	spawnFeeders(16, wg, linesChan)
	postZipFile("yelp_academic_dataset_business.json.zip", "business", linesChan, chunksize)
	postZipFile("yelp_academic_dataset_review.json.zip", "reviews", linesChan, chunksize)
	close(linesChan)
	wg.Wait()
}
