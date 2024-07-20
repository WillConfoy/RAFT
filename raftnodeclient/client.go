package main

import (
	"context"
	"flag"
	"log"
	// "time"
	"net"
	"fmt"
	"strings"
	"bufio"
	"os"
	"time"

	// "math/rand"

	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"

	// node "cs498.com/raft/raftnode"
	rs "cs498.com/raft/raftproto"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/reflection"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addrstring = flag.String("addrs", "127.0.0.1:50051", "A space separated list of addresses with ports. Ex: 127.0.0.1:50051")
	// myport = flag.String("port", "50051", "The port for this node to run on")
	myaddr = GetLocalIP()
)

func GetLocalIP() string {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return ""
    }
    for _, address := range addrs {
        // check the address type and if it is not a loopback the display it
        if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                return ipnet.IP.String()
            }
        }
    }
    return ""
}

func sendCommand(addrs []string, command string) {
	for _, x := range addrs {
		conn, err := grpc.Dial(x, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Error connecting to %s, error %v", x, err)
			continue
		}

		client := rs.NewRaftServiceClient(conn)

		response, err := client.MakeCommand(context.Background(), &rs.CommandRequest{
			Ip: myaddr,
			Command: command,
		})
		if err != nil {
			log.Printf("Error connecting to %s, error %v", x, err)
			continue
		} else {
			log.Printf("Got response: %v", response.GetResult())
			break
		}
	}
}

func main() {
	fmt.Println("Hi")
	flag.Parse()
	addrs := strings.Split(*addrstring, " ")
	
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		// text = strings.TrimSuffix(text, "\n")
		time.Sleep(1 * time.Second)
		sendCommand(addrs, text)
	}


}

// go run . -addrs "localhost:50052 localhost:50053 localhost:50054 localhost:50055" -port "50051"
// go run . -addrs "localhost:50051 localhost:50053 localhost:50054 localhost:50055" -port "50052"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50054 localhost:50055" -port "50053"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50053 localhost:50055" -port "50054"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50053 localhost:50054" -port "50055"


// go run . -addrs "localhost:50051 localhost:50052 localhost:50053 localhost:50054 localhost:50055"
