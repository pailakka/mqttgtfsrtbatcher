package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// github.com/davecgh/go-spew/spew

type MQTTPayload struct {
	Topic   string
	Payload []byte
}

func sendPayloadToMQTT(client mqtt.Client) func(payload MQTTPayload) {
	return func(payload MQTTPayload) {
		fmt.Println("Sending to \"", payload.Topic, "\" ", len(payload.Payload), " bytes")
		token := client.Publish(payload.Topic, 0, false, payload.Payload)
		token.Wait()
	}
}

func main() {
	gb := GTFSRTBatch{}
	quitChan := make(chan bool)

	mqtt.ERROR = log.New(os.Stdout, "", 0)
	ingressMQTTOpts := mqtt.NewClientOptions().AddBroker("tcp://mqtt.cinfra.fi:1883").SetClientID("mqttgtfsrtbatcher")
	ingressMQTTOpts.SetKeepAlive(2 * time.Second)
	ingressMQTTOpts.SetDefaultPublishHandler(gb.MessageHandler)
	ingressMQTTOpts.SetPingTimeout(1 * time.Second)

	ingressMQTTClient := mqtt.NewClient(ingressMQTTOpts)
	if token := ingressMQTTClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	outputMQTTOpts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883").SetClientID("mqttgtfsrtbatcher2")
	outputMQTTOpts.SetKeepAlive(2 * time.Second)
	outputMQTTOpts.SetDefaultPublishHandler(gb.MessageHandler)
	outputMQTTOpts.SetPingTimeout(1 * time.Second)

	outputMQTTClient := mqtt.NewClient(outputMQTTOpts)
	if token := outputMQTTClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	gb.Init(quitChan, sendPayloadToMQTT(outputMQTTClient))

	if token := ingressMQTTClient.Subscribe("gtfsrt/v1/fi/hsl/tu", 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	var end_waiter sync.WaitGroup
	end_waiter.Add(1)
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt)
	go func() {
		<-signal_channel
		end_waiter.Done()
	}()
	end_waiter.Wait()
	quitChan <- true
}
