package main

import (
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var (
	hosts = []string{"localhost:9093", "localhost:9094"}
	topic = "testCluster"
)

func createProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 2
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Flush.Frequency = time.Millisecond * 100
	config.Version = sarama.V0_10_0_1
	config.Net.DialTimeout = time.Second
	config.Net.ReadTimeout = time.Second
	config.Net.WriteTimeout = time.Second
	config.Net.KeepAlive = 30 * time.Second

	rawProducer, err := sarama.NewAsyncProducer(hosts, config)
	if err != nil {
		log.Printf("cannot connect to kafka (%s)", err)
		return nil
	}
	log.Printf("kafka async producer connected (hosts:%s)", hosts)

	return rawProducer
}

func consumeResponses(producer sarama.AsyncProducer) {
	go func() {
		for success := range producer.Successes() {
			log.Printf("written to partition:%d offset:%d", success.Partition, success.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("error err:%s", err.Err)
		}
	}()

}

func main() {
	message := "testMessage-"
	producer := createProducer()
	consumeResponses(producer)
	for i := 0; i < 5; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(message + strconv.Itoa(i)),
		}

		time.Sleep(time.Second)
	}
}
