package client

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// prometheus metrics
// var promSenderCounter = promauto.NewCounterVec(prometheus.CounterOpts{
// 	Name: "nats_client",
// }, []string{"subject"})

type NatsClient struct {
	Conn *nats.Conn
	Js   nats.JetStreamContext
}

func NewNatsClient(endpoint string) *NatsClient {
	endpoint = "nats://" + endpoint
	nc, err := nats.Connect(endpoint)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("connected to nats endpoint", endpoint)
	log.Println("nats version:", nc.ConnectedServerVersion())

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	return &NatsClient{
		Conn: nc,
		Js:   js,
	}
}

func (nc *NatsClient) AddJSPublisher(ch chan []byte, subject string) {
	go func() {
		for msg := range ch {
			_, err := nc.Js.PublishAsync(subject, msg)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()
}

func (nc *NatsClient) AddStream(streamName string, subjectNames []string, ageLimit time.Duration) {
	streamConfig := &nats.StreamConfig{
		Name:      streamName,
		Subjects:  subjectNames,
		Retention: nats.WorkQueuePolicy,
		MaxAge:    ageLimit,
	}
	_, err := nc.Js.AddStream(streamConfig)
	if err != nil {
		if err == nats.ErrStreamNameAlreadyInUse {
			log.Printf("stream %v already exists, updating stream config\n", streamName)
			_, err := nc.Js.UpdateStream(streamConfig)
			if err != nil {
				log.Fatal(err)
			}
			return
		}
		log.Fatal(err)
	}
}
