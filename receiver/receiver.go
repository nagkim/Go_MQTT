package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Foo struct {
	IntArray    []int     `json:"intArray"`
	FloatArray  []float32 `json:"floatArray"`
	StringArray []string  `json:"stringarray"`
}

var masterBrokerAddress = "tcp://10.37.129.2:61616"
var queueName = "test/mqtt"
var username = "admin"
var password = "admin"

func main() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(masterBrokerAddress)
	opts.SetUsername(username)
	opts.SetPassword(password)

	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)

		// Handle reconnection here
		for {
			opts := mqtt.NewClientOptions()
			opts.SetAutoReconnect(true) // Enable auto-reconnect
			opts.SetUsername(username)
			opts.SetPassword(password)

			opts.AddBroker(masterBrokerAddress)

			client := mqtt.NewClient(opts)

			if token := client.Connect(); token.Wait() && token.Error() != nil {
				fmt.Printf("Error reconnecting: %v\n", token.Error())
				time.Sleep(5 * time.Second) // Wait before retrying
				continue
			}

			fmt.Println("Reconnected successfully")
			break
		}
	}

	client := mqtt.NewClient(opts)

	// Connect to the MQTT broker
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		return
	}

	defer client.Disconnect(250)

	// Create a channel to receive incoming messages
	messageChannel := make(chan mqtt.Message)

	// Define a callback function to handle incoming messages
	messageHandler := func(client mqtt.Client, msg mqtt.Message) {
		messageChannel <- msg
	}

	// Subscribe to the MQTT topic
	if token := client.Subscribe(queueName, 0, messageHandler); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		return
	}

	defer client.Unsubscribe(queueName)

	// Wait for incoming messages
	for {
		select {
		case msg := <-messageChannel:
			// Decompress the received data
			var decompressedData bytes.Buffer
			gzipReader, err := gzip.NewReader(bytes.NewReader(msg.Payload()))
			if err != nil {
				log.Fatal("Error creating gzip reader:", err)
			}
			_, err = decompressedData.ReadFrom(gzipReader)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			gzipReader.Close()

			// Unmarshal the JSON data into a struct
			var receivedData map[string]interface{}
			err = json.Unmarshal(decompressedData.Bytes(), &receivedData)
			if err != nil {
				log.Fatal("Error unmarshaling JSON:", err)
			}

			// Process the received data as needed
			// For example, you can access it like this:
			fmt.Printf("Received Data:\n%v\n", receivedData)

		case <-time.After(1000 * time.Second):
			fmt.Println("No messages received for 1000 seconds. Exiting.")
			return
		}
	}
}
