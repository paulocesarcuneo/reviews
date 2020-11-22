package encoding

import (
	"bytes"
	"encoding/gob"
)

type Encoder func(interface{}) ([]byte, error)
type Decoder func([]byte) (interface{}, error)

type Message struct {
	Content interface{}
}

func GobEncoder(data interface{}) ([]byte, error) {
	var bytes bytes.Buffer
	encoder := gob.NewEncoder(&bytes)
	err := encoder.Encode(Message{Content: data})
	if err != nil {
		return nil, err
	}
	return bytes.Bytes(), nil
}

func GobDecoder(data []byte) (interface{}, error) {
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	var msg Message
	err := decoder.Decode(&msg)
	if err != nil {
		return nil, err
	}
	return msg.Content, nil
}
