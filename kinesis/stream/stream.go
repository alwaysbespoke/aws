package stream

import (
	"crypto/rand"
	"errors"
	"io"
	"math/big"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	MAX = "340282366920938463463374607431768211456"
)

// Emitter ...
//
// awsSession	AWS session
// client		Kinesis client
type Emitter struct {
	awsSession *session.Session
	client     *kinesis.Kinesis
	lock       sync.Mutex
	max        *big.Int
	reader     io.Reader
}

// New ...
//
// Creates an instance of Emitter that can be reused
func New(region string) (*Emitter, error) {

	var err error
	var e Emitter

	// set max
	max := new(big.Int)
	max, ok := max.SetString(MAX, 10)
	if !ok {
		return nil, errors.New("Partition key error")
	}
	e.max = max

	// set reader
	e.reader = rand.Reader

	// create AWS session
	e.awsSession, err = session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	// create Kinesis client
	e.client = kinesis.New(e.awsSession)

	return &e, nil

}

// PutRecord ...
//
// Puts a record into the specified stream and returns and error
func (e *Emitter) PutRecord(record []byte, stream string) (*kinesis.PutRecordOutput, error) {

	// create input
	var input kinesis.PutRecordInput
	input.Data = record
	input.StreamName = aws.String(stream)
	input.PartitionKey = aws.String(e.createPartitionKey())

	// lock
	e.lock.Lock()
	defer e.lock.Unlock()

	// put record
	output, err := e.client.PutRecord(&input)
	if err != nil {
		return nil, err
	}

	return output, nil

}

// Creates a key that balances shard load
func (e *Emitter) createPartitionKey() string {
	n, err := rand.Int(e.reader, e.max)
	if err != nil {
		return "0"
	}
	return n.String()
}
