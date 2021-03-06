package report

import (
	"log"
	"reviews/api"
	"reviews/pipe"
	"reviews/utils"
	"sync"
)

type Report struct {
	Histogram          map[string]int
	Top10Funniest      []api.StringInt
	Reviews50Plus      map[string]int
	Reviews50FiveStars []string
	Reviews5SameText   []string
}

func LaunchReportWorker(wg *sync.WaitGroup) (*Report, func() error) {
	report := Report{
		Histogram:          make(map[string]int),
		Reviews50FiveStars: []string{},
		Reviews5SameText:   []string{},
		Reviews50Plus:      make(map[string]int),
		Top10Funniest:      []api.StringInt{},
	}
	fifty50Stars5, fsCloser, err := pipe.Summary50Reviews5Stars.In()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for stars := range fifty50Stars5 {
			if len(stars) != 0 {
				report.Reviews50FiveStars = stars
			}
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
			if len(same) != 0 {
				report.Reviews5SameText = same
			}
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
			if len(fifty) != 0 {
				report.Reviews50Plus = utils.CopyN(fifty, 100)
			}
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
			if len(histo) != 0 {
				report.Histogram = histo
			}
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
			if len(city) != 0 {
				report.Top10Funniest = city
			}
		}
	}()

	return &report, func() error {
		tcCloser.Close()
		hCloser.Close()
		fpCloser.Close()
		fsCloser.Close()
		ftCloser.Close()
		return nil
	}
}
