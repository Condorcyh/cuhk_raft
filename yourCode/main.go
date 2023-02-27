package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {

	// TODO: Implement this!

	//persistent state on all servers
	currentTerm int32            //
	votedFor    int32            //
	log         []*raft.LogEntry //

	//volatile state on all servers
	commitIndex int32 //最后一个被大多数节点都复制的日志的index

	//volatile state on leaders
	nextIndex  map[int32]int32 //下一次appendEntries应该从哪里开始尝试
	matchIndex map[int32]int32 //已知的某follower的log与leader的log最大匹配到哪个index

	//other needed
	kvStore           map[string]int32 //
	serverState       raft.Role        //节点的状态，有leader、follower和candidate
	nodeId            int32            //节点id
	leaderId          int32            //当前任期内的leader
	electionTimeout   int32
	heartBeatInterval int32
	commitChan        chan bool
	resetChan         chan bool
	mutex             sync.Mutex //用来上锁
	majoritySize      int
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		log:               nil,
		currentTerm:       0,
		votedFor:          -1,
		nextIndex:         make(map[int32]int32),
		matchIndex:        make(map[int32]int32),
		serverState:       raft.Role_Follower,
		kvStore:           make(map[string]int32),
		resetChan:         make(chan bool, 1),
		commitChan:        make(chan bool, 1),
		heartBeatInterval: int32(heartBeatInterval),
		electionTimeout:   int32(electionTimeout),
		majoritySize:      (len(nodeidPortMap)+1)/2 + 1,
		commitIndex:       0,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here !
	ctx := context.Background()
	go func() {
		for {
			switch rn.serverState {
			case raft.Role_Leader:
				for hostID, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						rn.mutex.Lock()
						//拿到nextLogIndex
						var nextLogIndex = rn.nextIndex[hostId]

						var prevLogIndex = 0
						if nextLogIndex > 1 {
							prevLogIndex = int(nextLogIndex - 1)
						}

						var prevLogTerm = 0
						if prevLogIndex > 0 {
							prevLogTerm = int(rn.log[prevLogIndex-1].Term)
						}

						//要添加的日志
						sendLog := make([]*raft.LogEntry, 0)
						if nextLogIndex > 0 {
							sendLog = append(sendLog, rn.log[nextLogIndex-1:]...)
						}
						rn.mutex.Unlock()
						//调用其他节点的AppendEntries rpc
						reply, err := client.AppendEntries(ctx, &raft.AppendEntriesArgs{
							From:         int32(nodeId),
							To:           hostId,
							Term:         rn.currentTerm,
							LeaderId:     int32(nodeId),
							PrevLogIndex: int32(prevLogIndex),
							PrevLogTerm:  int32(prevLogTerm),
							Entries:      sendLog,
							LeaderCommit: rn.commitIndex,
						})

						if err == nil {
							rn.mutex.Lock()
							if reply.Success {
								rn.matchIndex[hostId] = reply.MatchIndex
								rn.nextIndex[hostId] = reply.MatchIndex + 1
								flag := true
								for flag {
									count := 1
									for i := range rn.matchIndex {
										if rn.matchIndex[i] >= rn.commitIndex+1 {
											count++
										}
									}
									//当大多数节点接受了这个日志项后，进行commit
									if count >= rn.majoritySize {
										//更新commitIndex
										rn.commitIndex = rn.commitIndex + 1
										//进行commit
										rn.commitChan <- true
									} else if count < rn.majoritySize {
										flag = false
									}
								}
							} else {
								//nextIndex--
								rn.nextIndex[hostId]--
							}
							rn.mutex.Unlock()
						}
					}(hostID, client)
				}
				select {
				case <-time.After(time.Duration(rn.heartBeatInterval) * time.Millisecond):
					//
				case <-rn.resetChan:
					//Do nothing
				}

			case raft.Role_Follower:
				select {
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
					rn.serverState = raft.Role_Candidate
				case <-rn.resetChan:
					//Do nothing
				}

			case raft.Role_Candidate:
				//当节点是candidate时，需要向其他节点发出RequestVote rpc请求投票
				rn.mutex.Lock()
				//当前任期+1
				rn.currentTerm++
				//投票给自己
				rn.votedFor = int32(nodeId)
				//自己最后一个日志索引
				lastLogIndex := len(rn.log)
				//自己最后一个日志的任期
				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = int(rn.log[lastLogIndex-1].Term)
				}
				rn.mutex.Unlock()

				voteNum := 1
				for hostID, client := range hostConnectionMap {
					go func(hostId int32, c raft.RaftNodeClient) {
						reply, err := c.RequestVote(ctx, &raft.RequestVoteArgs{
							From:         int32(nodeId),
							To:           hostId,
							Term:         rn.currentTerm,
							LastLogIndex: int32(lastLogIndex),
							LastLogTerm:  int32(lastLogTerm),
						})
						if err == nil {
							if reply.VoteGranted && reply.Term == rn.currentTerm {
								rn.mutex.Lock()
								voteNum++
								rn.mutex.Unlock()
								//获得大多数票数
								if voteNum >= rn.majoritySize && rn.serverState == raft.Role_Candidate {
									rn.serverState = raft.Role_Leader
									//reset nextIndex and matchIndex
									for i := range rn.nextIndex {
										rn.nextIndex[i] = int32(len(rn.log))
									}
									for i := range rn.matchIndex {
										rn.matchIndex[i] = 0
									}
									// reset timer
									rn.resetChan <- true
								}
							} else if reply.Term > rn.currentTerm {
								// What if the other node has larger term?
								//失去candidate资格，变成follower
								rn.serverState = raft.Role_Follower
							}
						}
					}(hostID, client)
				}
				select {
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
					//
				case <-rn.resetChan:
					//Do nothing
				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply

	switch rn.serverState {
	case raft.Role_Leader:
		//将日志项添加到本地日志
		rn.log = append(rn.log, &raft.LogEntry{
			Term:  rn.currentTerm,
			Op:    args.Op,
			Key:   args.Key,
			Value: args.V,
		})

		//阻塞，等待commit完成
		<-rn.commitChan

		//实现日志项
		ret.CurrentLeader = rn.nodeId
		switch args.Op {
		case raft.Operation_Put:
			ret.Status = raft.Status_OK
			rn.kvStore[args.Key] = args.V
		case raft.Operation_Delete:
			_, ok := rn.kvStore[args.Key]
			if ok {
				ret.Status = raft.Status_OK
				delete(rn.kvStore, args.Key) //调用删除方法将该键值从store中删去
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		}
	case raft.Role_Follower:
		ret.Status = raft.Status_WrongNode
		ret.CurrentLeader = rn.votedFor
	case raft.Role_Candidate:
		ret.Status = raft.Status_WrongNode
		ret.CurrentLeader = rn.votedFor
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft.GetValueReply
	value, ok := rn.kvStore[args.Key]
	if ok {
		ret.Status = raft.Status_KeyFound
		ret.V = value
	} else {
		ret.Status = raft.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	var reply raft.RequestVoteReply
	reply.From = args.To
	reply.To = args.From

	//如果请求的节点任期比本节点任期小，拒绝投票
	if args.Term < rn.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rn.currentTerm {
		//说明节点已经投过票了
		if rn.votedFor != -1 {
			reply.VoteGranted = false
		} else {
			//要投的不是请求的节点
			if rn.votedFor != args.CandidateId {
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
			}
		}
	} else {
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		rn.votedFor = args.From
		rn.currentTerm = args.Term
		rn.serverState = raft.Role_Follower
		rn.resetChan <- true // reset timer
	}

	reply.Term = rn.currentTerm

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	var reply raft.AppendEntriesReply

	reply.From = args.To
	reply.To = args.From
	reply.Success = true

	//任期比自己小，拒绝
	if args.Term < rn.currentTerm {
		reply.Success = false
	} else {
		//prevLogIndex表示前一个日志的日志号
		if args.PrevLogIndex > 0 {
			//prevLogIndex比节点的长度还大，拒绝，肯定还要补
			if args.PrevLogIndex > int32(len(rn.log)) {
				reply.Success = false
			}
			//节点的日志项任期与请求的发生冲突，所以要删除后面的日志项
			if rn.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
				reply.Success = false
				rn.log = rn.log[:args.PrevLogIndex]
			}
		}
	}

	if reply.Success {
		rn.currentTerm = args.Term
		rn.votedFor = args.From
		//将请求的日志项添加到末尾
		rn.log = append(rn.log, args.Entries...)
		//更新matchIndex
		reply.MatchIndex = int32(int(args.PrevLogIndex) + len(args.Entries))

		//应用日志，并且更新commitIndex，即已提交日志号
		if args.LeaderCommit > rn.commitIndex {
			//应用日志，直到leaderCommit，即leader已提交的日志项
			var start int32
			//找到开始项
			if rn.commitIndex > 0 {
				start = rn.commitIndex - 1
			} else {
				start = 0
			}
			for i, entry := range rn.log[start:args.LeaderCommit] {
				//put或者delete
				if entry.Op == raft.Operation_Put {
					fmt.Printf("put的日志项索引是%d\n", i)
					rn.kvStore[entry.Key] = entry.Value
				}
				if entry.Op == raft.Operation_Delete {
					fmt.Printf("delete的日志项索引是%d\n", i)
					_, ok := rn.kvStore[entry.Key]
					if ok {
						delete(rn.kvStore, entry.Key)
					}
				}
			}
			rn.commitIndex = args.LeaderCommit
		}
		//状态变更，变成follower
		rn.serverState = raft.Role_Follower
		//重置计时器
		rn.resetChan <- true
	}

	reply.Term = rn.currentTerm

	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.Timeout
	rn.resetChan <- true
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.Interval
	rn.resetChan <- true
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
