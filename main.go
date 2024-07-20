package main

import (
	// "context"
	"flag"
	// "log"
	// "time"
	// "net"
	"fmt"
	"strings"
	// "math/rand"

	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"

	node "cs498.com/raft/raftnode"
)

var (
	addrstring = flag.String("addrs", "127.0.0.1:50051", "A space separated list of addresses with ports. Ex: 127.0.0.1:50051")
	myport = flag.String("port", "50051", "The port for this node to run on")
)

func main() {
	fmt.Println("Hi")
	flag.Parse()
	addrs := strings.Split(*addrstring, " ")
	// fmt.Println(addrs)
	// fmt.Println(*myport)
	node.Run(addrs, *myport)
}

// go run . -addrs "localhost:50052 localhost:50053 localhost:50054 localhost:50055" -port "50051"
// go run . -addrs "localhost:50051 localhost:50053 localhost:50054 localhost:50055" -port "50052"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50054 localhost:50055" -port "50053"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50053 localhost:50055" -port "50054"
// go run . -addrs "localhost:50051 localhost:50052 localhost:50053 localhost:50054" -port "50055"