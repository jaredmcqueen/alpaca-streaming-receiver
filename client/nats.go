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

func (nc *NatsClient) AddPublisher(ch chan []byte, subject string) {
	go func() {
		for msg := range ch {
			_, err := nc.Js.Publish(subject, msg)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()
}

func (nc *NatsClient) AddStream(subject string, ageLimit time.Duration) {
	_, err := nc.Js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Subjects: []string{subject},
		MaxAge:   ageLimit,
	})
	if err != nil {
		if err == nats.ErrStreamNameAlreadyInUse {
			log.Println("stream", subject, "already exists")
			return
		}
		log.Fatal(err)
	}
}
