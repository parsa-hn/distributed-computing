package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var messageCount int = 0
var ackCount int = 0

func main() {

	if len(os.Args) != 3 {
		fmt.Println("Wrong input format")
		os.Exit(1)
	}

	brokerAddress := os.Args[1]
	serverType := os.Args[2]

	broker, err := net.ResolveTCPAddr("tcp4", brokerAddress)
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, broker)
	checkError(err)

	for {

		sendMessage(conn)

		if serverType == "sync" {
			getAck(conn)
		}
		if serverType == "async" {
			go getAck(conn)
		}
	}
	os.Exit(0)
}

func getAck(conn net.Conn) {
	ack := make([]byte, 512)

	_, err := conn.Read(ack)
	if err != nil {
		fmt.Println("Acknowledge not recived for this message")
		return
	}

	if strings.HasPrefix(string(ack), "Broker queue is full!") {
		messageCount--
		fmt.Println("message with id", messageCount, "filed")
	} else {
		fmt.Println("Ack recieved for message ", ackCount)
		ackCount++
	}
}

func sendMessage(conn net.Conn) {
	input := ""
	fmt.Scanln(&input)

	if input == "exit" {
		os.Exit(1)
	}

	_, err := conn.Write([]byte(input))
	if err != nil {
		fmt.Println("Connection error during sending message")
		return
	}
	fmt.Println("Message with id", messageCount, "and text ", string(input), " sent")
	messageCount++
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
