package raftnode

import (
	"context"
	"fmt"
	"log"
	"net"
	"slices"
	"time"
	"math/rand"
	"sync"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/reflection"
	"google.golang.org/grpc/credentials/insecure"

	rs "cs498.com/raft/raftproto"
)

var (
	// deadChannel = make(chan string)
	heartbeatChannel = make(chan int32)
	requestChannel = make(chan LogEntry)
	m sync.Mutex
	n sync.Mutex
	test sync.Mutex
	timer = time.Now()
	timeModifier = 50 // makes heartbeats and whatnot X times slower
)

type LogEntry struct {
	term int32
	command string
}


type Node struct {
	Myaddr string
	Peers map[string]rs.RaftServiceClient
	currState string
	signalMap map[LogEntry]chan string

	rs.UnimplementedRaftServiceServer;

	// Persistent state
	currentTerm int32
	votedFor string
	logArr []*LogEntry
	statemachine []*LogEntry
	currentLeader string

	// Volatile state on all servers
	commitIndex int32
	lastApplied int32

	// Volatile state on leaders
	nextIndex map[string]int32
	matchIndex map[string]int32
}

func Run(addrs []string, port string) {
	log.Printf("Running on port %s", port)
	// myaddr := GetLocalIP() + ":" + port
	myaddr := "localhost:" + port
	addrs = slices.DeleteFunc(addrs, func(addr string) bool {return myaddr == addr})
	node := Node{Myaddr: myaddr, Peers: make(map[string]rs.RaftServiceClient)}
	node.initializestates(len(addrs))
	node.Start(addrs)
}

func (node *Node) initializestates(length int) {
	// Persistent state
	node.currentTerm = 0
	node.votedFor = ""
	node.logArr = make([]*LogEntry, 0)
	node.currState = "follower"
	node.signalMap = make(map[LogEntry]chan string)

	// Volatile state on all servers
	node.commitIndex = 0
	node.lastApplied = 0

	// Volatile state on leaders
	node.nextIndex = make(map[string]int32)
	node.matchIndex = make(map[string]int32)
}

// Got this directly from stackoverflow
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

func (node *Node) Start(addrs []string) error {
	// first we start listening
	go node.StartListening()

	// for now, wait a minute to let me run the commands everywhere
	time.Sleep(5 * time.Second)

	// second we populate peer map
	node.CreatePeers(addrs)

	node.Heartbeat()

	// we would register with some discovery service, just ignore this for now

	// do some task to start out with
	// for {
	// 	// node.BroadcastHeartbeat()
	// 	time.Sleep(1 * time.Minute)
	// 	for k := range node.Peers {
	// 		fmt.Printf("Peer: %s\n", k)
	// 	}
	// }
	return nil
}

func (node *Node) Heartbeat() {
	// go node.handleDisconnects()
	for {
		node.checkapplied()
		if node.currState == "follower" {
			log.Printf("Follower commitIndex is %d\n", node.commitIndex)
			log.Printf("Follower lastApplied is %d\n", node.lastApplied)
			select {
			case term := <- heartbeatChannel :
				// we received the heartbeat
				// node.
				timer = time.Now()
				if term > node.currentTerm {
					node.currentTerm = term
				}
			case <- time.After(time.Duration(500 + rand.Intn(150)) * time.Millisecond * time.Duration(timeModifier)) :
				log.Printf("CHANGING TO CANDIDATE\n")
				log.Printf("Printing array of logs...\n")
				PrettyPrint(node.logArr)
				node.currState = "candidate"

				node.currentTerm++

				node.votedFor = node.Myaddr

				electChan := make(chan bool)

				go node.TryElection(electChan)

				select {
				case elected := <- electChan :

					if elected {
						node.BecomeLeader()
					}

				case <- time.After(time.Duration(500 * time.Millisecond * time.Duration(timeModifier))) :
					n.Lock()
					electChan = nil
					n.Unlock()
					continue
				}
	
				
			}
		} else if node.currState == "leader" {
			// for {
				// since this thread is just doing heartbeat stuff
				// might need another goroutine to handle actually servicing requests

				// Consider making a channel to put requests into so that they can piggyback off of the heartbeats
				// just like how it's done in the demo. Just need to find a way to say if there is something in the
				// channel, take it, if not, do something else
				// maybe use select statement with default case?
				go node.findAndCommitN()
				// log.Printf("Printing array of logs...\n")
				// PrettyPrint(node.logArr)
				log.Printf("Leader commitIndex is %d\n", node.commitIndex)
				log.Printf("Leader lastApplied is %d\n", node.lastApplied)
				select {
				case req := <- requestChannel : // request channel will be a channel of logEntries (probably)
					// we need to add this to our log too
					// only reply once this has been applied to the state machine
					log.Printf("Printing New Log Entry...\n")
					PrettyPrint([]*LogEntry{&req})
					log.Printf("Printing current log state...\n")
					PrettyPrint(node.logArr)
					// var temp *LogEntry
					// temp = &LogEntry{term: req.term, command: req.command}
					temp := req

					log.Printf("Req term: %d", req.term)
					log.Printf("Req command: %s", req.command)

					node.logArr = append(node.logArr, &temp)

					log.Printf("Printing updated log state...\n")
					PrettyPrint(node.logArr)
					node.BroadcastMessage()
				default :
					node.BroadcastHeartbeat()
				}
				time.Sleep(20 * time.Millisecond * time.Duration(timeModifier))
			// }
		} else if node.currState == "candidate" {
			// election must have timed out
			// do stuff

			electChan := make(chan bool)

			go node.TryElection(electChan)

			select {
			case elected := <- electChan :

				if elected {
					node.BecomeLeader()
				}

			case <- time.After(time.Duration(500 * time.Millisecond) * time.Duration(timeModifier)) :
				n.Lock()
				electChan = nil
				n.Unlock()
				continue
			}
			
		} else {
			log.Fatalf("Fatal error found, currState is somehow %v, dying horribly now", node.currState)
		}
		// node.broadcastChannel("alive?")
		// time.After(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
	}
}

func (node *Node) findAndCommitN() {
	// for i, x := range node.logArr {
	for i := range node.logArr {
		if i < int(node.commitIndex) {continue}
		counter := 0
		for _, y := range node.matchIndex {
			if i <= int(y) {counter++}
		}

		if counter > (len(node.matchIndex) / 2) && node.logArr[i].term == node.currentTerm {
			// log.Printf("Printing State Machine...\n")
			// PrettyPrint(node.statemachine)
			log.Printf("Incrementng commitIndex to: %d", node.commitIndex + 1)
			node.commitIndex++
			// node.lastApplied++
			// log.Printf("Appending %s to state machine in commitN method...\n", x.command)
			// node.statemachine = append(node.statemachine, x)
			// node.signalMap[*x] <- "Finished command: " + node.statemachine[i].command
			// log.Printf("Printing new State Machine...\n")
			// PrettyPrint(node.statemachine)
		}
	}
}

func (node *Node) TryElection(electChan chan bool) {
	// basically, broadcast but for vote requests instead
	// important to do this concurrently, aggregate results using channels
	// important to break and become follower again if receive vote request
	// also figure out another timeout for the election
	resultChan := make(chan int)
	counter := 1
	elected := false

	for i, x := range node.Peers {
		go node.sendVoteRequests(i, x, resultChan)
	}

	for range len(node.Peers) {
		counter += <- resultChan
		if counter > (len(node.Peers) / 2) {
			elected = true
			break
		}
	}
	
	n.Lock()
	if elected && node.currState == "candidate" && electChan != nil {
		electChan <- true
	} else if electChan != nil {
		electChan <- false
	} 
	n.Unlock()
}

func (node *Node) sendVoteRequests(ip string, client rs.RaftServiceClient, resultChan chan int) {
	last_log_term := int32(0)
	last_log_index := int32(min(0, len(node.logArr) - 1))

	if last_log_index < 1 {
		last_log_term = node.currentTerm
	} else {
		last_log_term = node.logArr[last_log_index].term
	}
	result, err := client.RequestVote(context.Background(), &rs.VoteRequest{
		Term: node.currentTerm,
		CandidateId: node.Myaddr,
		LastLogIndex: int32(len(node.logArr) - 1),
		LastLogTerm: last_log_term,
	})

	if err != nil {
		log.Printf("Error connecting to %s, error %v", ip, err)
	} else {
		log.Printf("Success! Asked %s for vote. Got %t", ip, result.GetVoteGranted())

		if result.GetTerm() > node.currentTerm {
			node.currState = "follower"
		}

		if result.GetVoteGranted() {
			resultChan <- 1
		} else {
			resultChan <- 0
		}
	}
}

func (node *Node) BecomeLeader() {
	// TODO:
	// Initialize all states, send out heartbeat
	log.Printf("I AM BECOMING THE LEADER\n")
	log.Printf("MY TERM IS: %d", node.currentTerm)
	node.currState = "leader"
	node.currentLeader = node.Myaddr

	node.initializeleaderstates()

	node.BroadcastHeartbeat()
}

func (node *Node) initializeleaderstates() {
	for i := range node.Peers {
		node.nextIndex[i] = int32(len(node.logArr))
		node.matchIndex[i] = 0
	}
}

// consider this implementation
// func (node *Node) TestHeartbeat() {
// 	// go node.handleDisconnects()
// 	for {
// 		select {
// 		case <- deadChannel :
// 			// we received a vote request too early
// 			// this means we should ignore it
// 			// maybe doesn't work
// 			// would need to add channels to the voting logic
// 		case <- time.After(150 * time.Millisecond) :
// 			select {
// 			case <- heartbeatChannel :
// 				// we received the heartbeat
// 				// node.
// 			case <- time.After(time.Duration(rand.Intn(150)) * time.Millisecond) :
// 				// we timed out
// 			}
// 		}
// 		// node.broadcastChannel("alive?")
// 		// time.After(time.Duration(150 + rand.Intn(150)) * time.Millisecond)
// 	}
// }


// func (node *Node) handleDisconnects() {
// 	for {
// 		deadIp := <- deadChannel
// 		log.Printf("A PEER HAS DIED: %s", deadIp)
// 		delete(node.Peers, deadIp)
// 	}
// }

// func (node *Node) Broadcast(message string) {
// 	// slow but whatever, peer map should be small anyways
 
// 	for i, x := range node.Peers {

// 		// use goroutines and channels for this later
// 		res, err := x.Message(context.Background(), &rs.MessageRequest{Addr: node.Myaddr, Message: message})
// 		if err != nil {
// 			log.Printf("Error connecting to %s, error %v", i, err)
// 		} else {
// 			log.Printf("Response: Key is message: %s, and val is their addr: %s. Sanity check -> message: %s, addr: %s", res.GetAddr(), res.GetResponse(), message, i)
// 		}
// 	}
// }

func (node *Node) BroadcastHeartbeat() {
	// slow but whatever, peer map should be small anyways
 
	for i, x := range node.Peers {
		go node.actuallyHeartbeat(i, x)
	}
}

func (node *Node) actuallyHeartbeat(ip string, client rs.RaftServiceClient) {
	res, err := client.AppendEntries(context.Background(), &rs.AppendRequest{
		Term: node.currentTerm,
		LeaderId: node.Myaddr,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
		Entries: []*rs.Entries{},
		LeaderCommit: node.commitIndex,
	})
	if err != nil {
		log.Printf("Error connecting to %s, error %v", ip, err)
	} else {
		// log.Printf("Response: Key is message: %s, and val is their addr: %s. Sanity check -> message: %s, addr: %s", res.GetAddr(), res.GetResponse(), message, i)
		// log.Printf("Success is %t, term is %d", res.GetSuccess(), res.GetTerm())
		if res.GetTerm() > node.currentTerm {
			node.currState = "follower"
		}
	}
}


func (node *Node) actuallyBroadcast(ip string, client rs.RaftServiceClient, tempchan chan int) {
	// slow but whatever, peer map should be small anyways
	// test.Lock()
	for len(node.logArr) - 1 >= int(node.nextIndex[ip]) {
		toSend := node.logArr[node.nextIndex[ip]]
		entry := rs.Entries{Term: node.currentTerm, Command: toSend.command}
		log.Printf("The node.nextIndex value for %s is: %d\n", ip, node.nextIndex[ip])
		res, err := client.AppendEntries(context.Background(), &rs.AppendRequest{
			Term: node.currentTerm,
			LeaderId: node.Myaddr,
			PrevLogIndex: node.nextIndex[ip]-1,
			PrevLogTerm: toSend.term,
			Entries: []*rs.Entries{&entry},
			LeaderCommit: node.commitIndex,
		})
		log.Printf("THIS IS THE RESULT FOR %s: %t", ip, res.GetSuccess())
		if err != nil {
			log.Printf("Error connecting to %s, error %v", ip, err)
			break
		} else {
			log.Printf("Success: %t and Term: %d", res.GetSuccess(), res.GetTerm())
			if !res.Success {
				node.nextIndex[ip] -= 1
			} else { // It might be growing its own log as it does this
				node.matchIndex[ip] = node.nextIndex[ip]
				node.nextIndex[ip] += 1
				log.Printf("New nextIndex for ip: %s is %d\n", ip, node.nextIndex[ip])
			}
			// log.Printf("Response: Key is message: %s, and val is their addr: %s. Sanity check -> message: %s, addr: %s", res.GetAddr(), res.GetResponse(), message, i)
			log.Printf("Success? is %t, term is %d", res.GetSuccess(), res.GetTerm())
			if res.GetTerm() > node.currentTerm {
				node.currState = "follower"
				break
			}
		}
	}
	tempchan <- 1
	// test.Unlock()
}

func (node *Node) BroadcastMessage() {
	// slow but whatever, peer map should be small anyways
	tempchan := make(chan int)
	for i, x := range node.Peers {
		log.Printf("Sending request to: %s", i)
		go node.actuallyBroadcast(i, x, tempchan)
	}

	for range node.Peers {
		<- tempchan
	}


}

// func (node *Node) broadcastChannel(message string) {
// 	// slow but whatever, peer map should be small anyways

// 	for i, x := range node.Peers {

// 		// use goroutines and channels for this later
// 		_, err := x.Message(context.Background(), &rs.MessageRequest{Addr: node.Myaddr,Message: message})
// 		if err != nil {
// 			log.Printf("Error connecting to %s, error %v", i, err)
// 			deadChannel <- i
// 		}
// 	}

// }

func (node *Node) CreatePeers(addrs []string) {
	for _,x := range addrs {
		node.CreateClient(x)
	}
}

func (node *Node) CreateClient(addr string) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Error connecting to %s, error %v", addr, err)
	}

	// defer conn.Close()

	node.Peers[addr] = rs.NewRaftServiceClient(conn)

	response, err := node.Peers[addr].Message(context.Background(), &rs.MessageRequest{Addr: node.Myaddr, Message: "Creating connection: " + node.Myaddr})
	if err != nil {
		log.Printf("Error connecting to %s, error %v", addr, err)
	}

	log.Printf("Greeting from other node: %s", response.GetResponse())
}


func (node *Node) StartListening() {
	lis, err := net.Listen("tcp", node.Myaddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer() // n is for serving purpose

	rs.RegisterRaftServiceServer(grpcServer, node)
	// reflection.Register(grpcServer)

	// start listening
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// func (node *Node) checkapplied() {
// 	if node.commitIndex > node.lastApplied {
// 		diff := node.commitIndex - node.lastApplied
// 		for i := range diff {
// 			// node.statemachine = append(node.statemachine, node.logArr[node.lastApplied + i + 1])
// 			log.Printf("Appending %s to state machine in checkapplied method...\n", node.logArr[node.lastApplied + i].command)
// 			node.statemachine = append(node.statemachine, node.logArr[node.lastApplied + i])
// 			node.signalMap[*node.logArr[node.lastApplied + i]] <- "Finished command: " + node.statemachine[i].command
// 		}
// 		node.lastApplied = node.commitIndex
// 	}
// }

func (node *Node) checkapplied() {
	if node.commitIndex > node.lastApplied {
		log.Printf("Appending %s to state machine in checkapplied method...\n", node.logArr[node.lastApplied].command)
		node.statemachine = append(node.statemachine, node.logArr[node.lastApplied])
		node.lastApplied++
		log.Printf("New leader lastApplied is: %d\n", node.lastApplied)
		if node.currState == "leader" {
			log.Printf("I am the leader right now\n")
			node.signalMap[*node.logArr[node.lastApplied-1]] <- "Finished command: " + node.statemachine[len(node.statemachine) - 1].command
		}
	}
}

func (node *Node) Message(ctx context.Context, in *rs.MessageRequest) (*rs.MessageResponse, error) {
	// check if in network, add if not
	_, ok := node.Peers[in.GetAddr()]
	if !ok {
		log.Printf("New peer found / revived: %s\n", in.GetAddr())
		node.CreateClient(in.GetAddr())
	}
	// do one of several computations
	return &rs.MessageResponse{Addr: in.GetAddr(), Message: in.GetMessage(), Response: "Received " + in.GetMessage() + " from " + in.GetAddr()}, nil
}

func (node *Node) AppendEntries(ctx context.Context, in *rs.AppendRequest) (*rs.AppendResponse, error) {
	response := rs.AppendResponse{}

	if in.GetTerm() > node.currentTerm {
		node.currentTerm = in.GetTerm()
		node.currState = "follower"
		if node.currentLeader == node.Myaddr {node.currentLeader = in.GetLeaderId()}
	}

	response.Term = node.currentTerm
	response.Success = true
	node.votedFor = ""

	heartbeatChannel <- in.GetTerm()

	if in.GetLeaderCommit() > node.commitIndex {
		// set commitIndex = min(leaderCommit, index of last new entry)
		node.commitIndex = min(in.GetLeaderCommit(), int32(len(node.logArr)-1))
		// make sure to check the condition commitIndex > lastApplied and handle that
		// maybe make a function that does this
		node.checkapplied()
	}

	if len(in.GetEntries()) == 0 && in.GetTerm() >= node.currentTerm {
		node.currentLeader = in.GetLeaderId()
		return &response, nil
	}

	log.Printf("Printing array of logs...\n")
	PrettyPrint(node.logArr)


	if in.GetTerm() < node.currentTerm {
		response.Success = false
		return &response, nil
	} else if len(node.logArr) == 0 {
		node.currentLeader = in.GetLeaderId()
	} else if node.logArr[in.GetPrevLogIndex()].term != in.GetPrevLogTerm() {
		if in.GetLeaderId() != node.currentLeader {
			node.currentLeader = in.GetLeaderId()
		}
		response.Success = false
		return &response, nil
	}

	if in.GetLeaderId() != node.currentLeader {
		node.currentLeader = in.GetLeaderId()
		response.Success = false
		return &response, nil
	}

	// newEntries := convertEntriesToLogEntries(in.GetEntries())
	newEntry := LogEntry{term: in.GetTerm(), command: in.GetEntries()[0].GetCommand()}
	// counter := 0	

	// // do the thing where you see if entries conflict and fix them
	if len(node.logArr) != 0 && node.logArr[in.GetPrevLogIndex()].term != newEntry.term {
		node.logArr = node.logArr[:in.GetPrevLogIndex()+1]
	}

	// // WARNING: MAY HAVE OFF BY ONE ERRROR
	// if placeToSlice > -1 {node.logArr = node.logArr[:placeToSlice]}

	// // AFTER DOING THAT, APPEND THE ENTRIES
	// // now that we have trimmed the log, we want to append the new entries
	// // the ellipsis thing seems to work, called variadic function
	// // we slice with the counter to ensure that we aren't duplicating
	// // in the case that some of the new entries are there already but
	// // there's a conflict half way through
	// node.logArr = append(node.logArr, newEntries[counter:]...)

	node.logArr = append(node.logArr, &newEntry)

	return &response, nil
}


// func convertEntriesToLogEntries(in []*rs.Entries) ([]*LogEntry) {
// 	newLogEntries := []*LogEntry{}
// 	for _, x := range in {
// 		newLog := &LogEntry{term: x.GetTerm(), command: x.GetCommand()}
// 		newLogEntries = append(newLogEntries, newLog)
// 	}
// 	return newLogEntries
// }

func (node *Node) RequestVote(ctx context.Context, in *rs.VoteRequest) (*rs.VoteResponse, error) {
	response := rs.VoteResponse{}

	// if in.GetTerm() < int32(node.currentTerm) {
	// 	response.VoteGranted = false
	// } 

	if in.GetTerm() > node.currentTerm {
		node.currentTerm = in.GetTerm()
		node.currState = "follower"
	}

	response.Term = node.currentTerm

	// IMPLEMENT IGNORING IF THE MIN TIMEOUT HASN'T HAPPENED (150 ms)
	response.VoteGranted = false

	if time.Since(timer) < time.Duration(150 * time.Millisecond) {return &response, nil}

	if (node.votedFor == "" || node.votedFor == in.GetCandidateId()) {
		if node.checkIfMoreUpToDate(in) {
			node.votedFor = in.GetCandidateId()
			response.VoteGranted = true
		}
	}
	return &response, nil
}

// THIS IS CALLED BY CLIENTS TO ADD COMMANDS TO THE RAFT NETWORK
func (node *Node) MakeCommand(ctx context.Context, in *rs.CommandRequest) (*rs.CommandResponse, error) {
	// response := rs.CommandResponse{}

	// if in.GetTerm() < int32(node.currentTerm) {
	// 	response.VoteGranted = false
	// } 

	if node.currState != "leader" {
		// call this on the leader
		log.Printf("Current leader is: %v\n", node.currentLeader)
		if node.currentLeader == node.Myaddr {
			log.Fatalf("WHY ARE WE HERE RIGHT NOW!!!", node.currState)
		}
		res, err := node.Peers[node.currentLeader].MakeCommand(context.Background(), in)
		if err != nil {
			log.Printf("Error connecting to %s, error %v", node.currentLeader, err)
		}
		return res, nil
	} else {
		// gotta make a channel so that we can tell when it gets applied to the state machine
		// this'll just have to block while we wait, at which point we respond
		newLog := LogEntry{term: node.currentTerm, command: in.GetCommand()}

		m.Lock()
		node.signalMap[newLog] = make(chan string)
		m.Unlock()

		requestChannel <- newLog
		result := <- node.signalMap[newLog]

		return &rs.CommandResponse{Result: result}, nil
	}
}

func (node *Node) checkIfMoreUpToDate(in *rs.VoteRequest) bool {
	if node.commitIndex != int32(0) {
		log.Printf("Printing array of logs...\n")
		PrettyPrint(node.logArr)
		lastEntry := node.logArr[node.commitIndex]
		if lastEntry.term > in.GetLastLogTerm() {
			return false
		} else if lastEntry.term < in.GetLastLogTerm() {
			return true
		}
		
		if node.commitIndex > in.GetLastLogIndex() {
			return false
		} else {
			return true
		}
	}
	return true
}

func PrettyPrint(entries []*LogEntry) {
	toPrint := "["
	for _, x := range entries {
		toPrint = toPrint + " " + x.command
	}
	toPrint += " ]\n"
	fmt.Printf("%v", toPrint)
	fmt.Printf("Oh also the length is %d\n", len(entries))
}



// maybe if the entries array is empty
// we completely change the behavior
// stay in a while loop in a goroutine
// and the heartbeat sends info into a channel
// we block on the case from the channel
// either it's the heartbeat signal and it's fine
// or it's not and we've timed out