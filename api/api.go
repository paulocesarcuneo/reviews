package api

import (
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"time"
)

type Review struct {
	User_id     string
	Date        string
	Business_id string
	Text        string
	Stars       float64
	Emit        string
	Quit        string
}

func ParseDate(s string) time.Time {
	date, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return date
}

func MapUsers(reviews []Review) []string {
	var result []string
	for _, r := range reviews {
		result = append(result, r.User_id)
	}
	return result
}

func MapDate(reviews []Review) []time.Time {
	var result []time.Time
	for _, r := range reviews {
		result = append(result, ParseDate(r.Date))
	}
	return result
}

type Business struct {
	City        string
	Business_id string
}

type BusinessText struct {
	Business string
	Text     string
}

func MD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func MapBusinessText(reviews []Review) []BusinessText {
	var result []BusinessText
	for _, r := range reviews {
		result = append(result, BusinessText{Business: r.Business_id, Text: r.Text})
	}
	return result
}

type UserStars struct {
	User  string
	Stars float64
}

func MapUserStars(reviews []Review) []UserStars {
	var result []UserStars
	for _, r := range reviews {
		result = append(result, UserStars{User: r.User_id, Stars: r.Stars})
	}
	return result
}

type UserText struct {
	User string
	Text string
}

func MapUserText(reviews []Review) []UserText {
	var result []UserText
	for _, r := range reviews {
		result = append(result, UserText{User: r.User_id, Text: MD5Hash(r.Text)})
	}
	return result
}

type StringInt struct {
	String string
	Int    int
}
type ByCounter []StringInt

func (l ByCounter) Len() int           { return len(l) }
func (l ByCounter) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l ByCounter) Less(i, j int) bool { return l[i].Int > l[j].Int }

//////////////////////////////////////////////////
type Signal struct {
	Action string
	Name   string
}

var Quit = Signal{Action: "quit"}
var Emit = Signal{Action: "emit"}

const Five = 5.0

func init() {
	gob.Register(Review{})
	gob.Register([]Review{})
	gob.Register(Business{})
	gob.Register([]Business{})
	gob.Register(BusinessText{})
	gob.Register([]BusinessText{})
	gob.Register(UserStars{})
	gob.Register([]UserStars{})
	gob.Register(UserText{})
	gob.Register([]UserText{})
	gob.Register(StringInt{})
	gob.Register([]StringInt{})
	gob.Register(time.Now())
	gob.Register(Signal{})
	gob.Register([]string{})
	gob.Register([]time.Time{})
	gob.Register([]interface{}{})
	gob.Register(make(map[string]int))
	gob.Register(make(map[string]bool))
}
