package utils

import (
	"log"
	"time"
)

func Retry(times int, fn func() (interface{}, error)) (interface{}, error) {
	var val interface{}
	var err error
	for i := 0; i < times; i++ {
		val, err = fn()
		if err == nil || i == times-1 {
			break
		}
		sleepSecond := 1 << i
		log.Println("Retry: ", i, err, "sleep for", sleepSecond)
		time.Sleep(time.Duration(sleepSecond) * time.Second)
	}
	return val, err
}

func Pool(threads int, fn func()) {
	for i := 0; i < threads; i++ {
		go fn()
	}
}

func Copy(in map[string]int) map[string]int {
	result := make(map[string]int)
	for k, v := range in {
		result[k] = v
	}
	return result
}

func CopyN(in map[string]int, N int) map[string]int {
	result := make(map[string]int)
	for k, v := range in {
		if N <= 0 {
			break
		}
		result[k] = v
		N--
	}
	return result
}
