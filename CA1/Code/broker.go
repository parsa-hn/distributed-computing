package main

import (
	"fmt"
	"net"
	"os"
)

const queueSize int = 10

var channel chan string = make(chan string, queueSize)
var serverCon net.Conn

func runBroker(conn net.Conn) {
	message := <-channel
	conn.Write([]byte(message))
	serverCon.Write([]byte("ack"))
	conn.Close()
	fmt.Println("Message delivered")
}

func connectToClient(clientAddress string) {
	fmt.Println("Broker is listening for new clients...")
	service := clientAddress
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err == nil {
			fmt.Println("New client connected!")
			go runBroker(conn)
		}
	}
}

func connectToServer(serverAddress string) net.Conn {
	fmt.Println("Broker is listening for a server...")
	service := serverAddress
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err == nil {
			fmt.Println("Server connected!")
			return conn
		}
	}
}

func readMessage() {
	for {
		message := make([]byte, 512)
		serverCon.Read(message)
		if len(channel) == queueSize {
			serverCon.Write([]byte("Broker queue is full!"))
		} else {
			channel <- string(message)
		}
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Wrong input format")
		os.Exit(1)
	}
	serverAddress := os.Args[1]
	clientAddress := os.Args[2]

	serverCon = connectToServer(serverAddress)

	go readMessage()

	connectToClient(clientAddress)

	close(channel)
}
