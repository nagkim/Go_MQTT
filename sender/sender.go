package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Foo struct {
	IntArray    []int     `json:"intArray"`
	FloatArray  []float32 `json:"floatArray"`
	StringArray []string  `json:"stringarray"`
}

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())

	// Set of characters to choose from
	chars := "abcdefghijklmnopqrstuvwxyz"

	// Generate the random string
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}

func main() {
	brokerAddress := "tcp://10.37.129.2:61616"
	username := "admin"
	password := "admin"
	queueName := "test/mqtt"
	arraySize := 10

	// Create an MQTT client options
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerAddress)
	opts.SetUsername(username)
	opts.SetPassword(password)

	// Create an MQTT client
	client := mqtt.NewClient(opts)

	// Connect to the MQTT broker
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	defer client.Disconnect(250)

	// Create a JSON representation of your data
	jsonMessage := map[string]interface{}{

		"intArray":    make([]int32, arraySize),
		"floatArray":  make([]float32, arraySize),
		"stringArray": make([]string, arraySize),
	}

	// Create a ticker that ticks every 1 second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Fill the int array with values from 1 to arraySize
			for i := 0; i < arraySize; i++ {
				jsonMessage["intArray"].([]int32)[i] = int32(i + 1)
			}

			// Fill the float array with values from 1 to arraySize
			for i := 0; i < arraySize; i++ {
				jsonMessage["floatArray"].([]float32)[i] = float32(i + 1)
			}

			// Fill the string array with random strings
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < arraySize; i++ {
				// Generate a random string of length 10
				randomString := generateRandomString(10)
				jsonMessage["stringArray"].([]string)[i] = randomString
			}

			// Marshal the Foo struct into JSON
			jsonData, err := json.Marshal(jsonMessage)
			if err != nil {
				fmt.Println("Error marshaling JSON:", err)
				os.Exit(1)
			}

			// Gzip compress the JSON data
			var compressedData bytes.Buffer
			gzipWriter := gzip.NewWriter(&compressedData)
			_, err = gzipWriter.Write(jsonData)
			if err != nil {
				log.Fatal("Error writing compressed data:", err)
			}
			gzipWriter.Close()

			// Publish the zipped data to the MQTT queue
			token := client.Publish(queueName, 0, false, compressedData)
			token.Wait()

			if token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}

			fmt.Printf("JSON data sent\n")
		}
	}
}
