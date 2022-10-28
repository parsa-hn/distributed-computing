package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Wrong input format")
		os.Exit(1)
	}
	brokerAddress := os.Args[1]

	var sleep_delay int
	task := make([]byte, 512)

	broker, err := net.ResolveTCPAddr("tcp4", brokerAddress)
	checkError(err)
	for true {
		conn, err := net.DialTCP("tcp", nil, broker)
		checkError(err)

		_, err = conn.Write([]byte("woker ready"))
		checkError(err)

		_, err = conn.Read(task)
		if err != nil {
			continue
		}

		if strings.HasPrefix(string(task), "close") {
			fmt.Println("Client closed!")
			break
		}

		sleep_delay = rand.Intn(10) + 1
		fmt.Println("Message", string(task), "recieved and now sleeping for", sleep_delay, "seconds")
		time.Sleep(time.Duration(sleep_delay) * time.Second)
	}
	os.Exit(0)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
