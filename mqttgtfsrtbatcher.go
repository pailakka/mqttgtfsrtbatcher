package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
)

// github.com/davecgh/go-spew/spew

type MQTTPayload struct {
	Topic   string
	Payload []byte
}

func sendPayloadToMQTT(client *mqtt.Client) func(payload MQTTPayload) {
	return func(payload MQTTPayload) {
		fmt.Println("Sending to \"", payload.Topic, "\" ", len(payload.Payload), " bytes")
		token := (*client).Publish(payload.Topic, 0, false, payload.Payload)
		token.Wait()
	}
}

type mqttgtfsrtbatcher struct {
	IngressMQTTOpts *mqtt.ClientOptions
	OutputMQTTOpts  *mqtt.ClientOptions

	IngressMQTTClient mqtt.Client
	OutputMQTTClient  mqtt.Client
}

func newBatcher(gtfsrtbathcer *GTFSRTBatch, IngressBrokerURI string, OutputBrokerURI string) mqttgtfsrtbatcher {
	mqtt.ERROR = log.New(os.Stdout, "", 0)

	ingressURLParsed, err := url.Parse(OutputBrokerURI)
	if err != nil {
		panic(err)
	}

	ingressMQTTOpts := mqtt.NewClientOptions().AddBroker(IngressBrokerURI).SetClientID("mqttgtfsrtbatcher")
	ingressMQTTOpts.SetKeepAlive(2 * time.Second)
	ingressMQTTOpts.SetDefaultPublishHandler(gtfsrtbathcer.MessageHandler)
	ingressMQTTOpts.SetPingTimeout(1 * time.Second)

	if ingressURLParsed.User != nil {
		ingressMQTTOpts.SetCredentialsProvider(func() (username string, password string) {
			pass, _ := ingressURLParsed.User.Password()
			return ingressURLParsed.User.Username(), pass
		})
	}

	outputURLParsed, err := url.Parse(OutputBrokerURI)
	if err != nil {
		panic(err)
	}

	outputMQTTOpts := mqtt.NewClientOptions().AddBroker(OutputBrokerURI).SetClientID("mqttgtfsrtbatcher")
	outputMQTTOpts.SetKeepAlive(2 * time.Second)
	outputMQTTOpts.SetDefaultPublishHandler(gtfsrtbathcer.MessageHandler)
	outputMQTTOpts.SetPingTimeout(1 * time.Second)
	if outputURLParsed.User != nil {
		outputMQTTOpts.SetCredentialsProvider(func() (username string, password string) {
			pass, _ := outputURLParsed.User.Password()
			return outputURLParsed.User.Username(), pass
		})
	}

	mgb := mqttgtfsrtbatcher{ingressMQTTOpts, outputMQTTOpts, nil, nil}

	mgb.IngressMQTTClient = mqtt.NewClient(ingressMQTTOpts)
	if token := mgb.IngressMQTTClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	mgb.OutputMQTTClient = mqtt.NewClient(outputMQTTOpts)
	if token := mgb.OutputMQTTClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return mgb
}

func main() {
	argsWithoutProg := os.Args[1:]

	gb := GTFSRTBatch{}
	quitChan := make(chan bool)

	mqttbatcher := newBatcher(&gb, argsWithoutProg[0], argsWithoutProg[1])

	gb.Init(quitChan, sendPayloadToMQTT(&(mqttbatcher.OutputMQTTClient)))

	if token := mqttbatcher.IngressMQTTClient.Subscribe("gtfsrt/v1/fi/hsl/tu", 0, nil); token.Wait() && token.Error() != nil {
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
