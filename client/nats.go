package client

import (
	"bytes"
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// prometheus metrics
var promSenderCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nats_client",
}, []string{"subject"})

type natsClient struct {
	conn nats.JetStreamContext
}

func NewNatsClient(endpoint string) (*natsClient, error) {
	nc, err := nats.Connect("nats://" + endpoint)
	if err != nil {
		return nil, err
	}

	conn, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	sender := natsClient{
		conn: conn,
	}

	return &sender, nil
}

func (s *natsClient) AttachWriter(ch chan any, subject string) {
	log.Println("attaching nats writer for", subject)
	go func() {
		for message := range ch {
			promSenderCounter.WithLabelValues(subject).Inc()
			payload, err := encodeJSON(message)
			if err != nil {
				log.Println("JSON Encode", err)
			}
			_, err = s.conn.PublishAsync(subject, payload)
			if err != nil {
				log.Println("PublishAsync", err)
			}
		}
	}()
}

func encodeJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	err := json.NewEncoder(&b).Encode(v)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (s *natsClient) AddStream(streamName string, subjects []string) {
	_, err := s.conn.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: subjects,
	})
	if err != nil {
		if err == nats.ErrStreamNameAlreadyInUse {
			log.Println("stream", streamName, "already exists")
			return
		}
		log.Fatal(err)
	}
}
