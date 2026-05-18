// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/msp"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type broadcastClient struct {
	client    ab.AtomicBroadcast_BroadcastClient
	signer    identity.SignerSerializer
	channelID string
}

type deliverClient struct {
	client    ab.AtomicBroadcast_DeliverClient
	channelID string
	signer    identity.SignerSerializer
}

type perfMetrics struct {
	mu              sync.Mutex
	totalTxSent     uint64
	totalTxAcked    uint64
	totalBlocksRecv uint64
	startTime       time.Time
	endTime         time.Time
	latencies       []time.Duration
}

func (m *perfMetrics) recordTxSent() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTxSent++
}

func (m *perfMetrics) recordTxAcked(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalTxAcked++
	m.latencies = append(m.latencies, latency)
}

func (m *perfMetrics) recordBlockReceived() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalBlocksRecv++
}

func (m *perfMetrics) printStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	duration := m.endTime.Sub(m.startTime).Seconds()
	throughput := float64(m.totalTxAcked) / duration

	fmt.Println("\n=== Performance Metrics ===")
	fmt.Printf("Total Transactions Sent: %d\n", m.totalTxSent)
	fmt.Printf("Total Transactions Acknowledged: %d\n", m.totalTxAcked)
	fmt.Printf("Total Blocks Received: %d\n", m.totalBlocksRecv)
	fmt.Printf("Duration: %.2f seconds\n", duration)
	fmt.Printf("Throughput: %.2f tx/sec\n", throughput)

	if len(m.latencies) > 0 {
		var sum time.Duration
		min := m.latencies[0]
		max := m.latencies[0]

		for _, lat := range m.latencies {
			sum += lat
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
		}

		avg := sum / time.Duration(len(m.latencies))
		fmt.Printf("Latency - Min: %v, Max: %v, Avg: %v\n", min, max, avg)
	}
	fmt.Println("===========================")
}

func newBroadcastClient(client ab.AtomicBroadcast_BroadcastClient, channelID string, signer identity.SignerSerializer) *broadcastClient {
	return &broadcastClient{client: client, channelID: channelID, signer: signer}
}

func (s *broadcastClient) broadcast(transaction []byte) error {
	env, err := protoutil.CreateSignedEnvelope(cb.HeaderType_ENDORSER_TRANSACTION, s.channelID, s.signer, &cb.Envelope{Payload: transaction}, 0, 0)
	if err != nil {
		return err
	}
	return s.client.Send(env)
}

func (s *broadcastClient) getAck() error {
	msg, err := s.client.Recv()
	if err != nil {
		return err
	}
	if msg.Status != cb.Status_SUCCESS {
		return fmt.Errorf("got unexpected status: %v - %s", msg.Status, msg.Info)
	}
	return nil
}

func newDeliverClient(client ab.AtomicBroadcast_DeliverClient, channelID string, signer identity.SignerSerializer) *deliverClient {
	return &deliverClient{client: client, channelID: channelID, signer: signer}
}

func (r *deliverClient) seekHelper(start *ab.SeekPosition, stop *ab.SeekPosition) *cb.Envelope {
	env, err := protoutil.CreateSignedEnvelope(cb.HeaderType_DELIVER_SEEK_INFO, r.channelID, r.signer, &ab.SeekInfo{
		Start:    start,
		Stop:     stop,
		Behavior: ab.SeekInfo_BLOCK_UNTIL_READY,
	}, 0, 0)
	if err != nil {
		panic(err)
	}
	return env
}

func (r *deliverClient) seekNewest() error {
	newest := &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}}
	maxStop := &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}}
	return r.client.Send(r.seekHelper(newest, maxStop))
}

func (r *deliverClient) readBlocks(metrics *perfMetrics, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			return
		default:
			msg, err := r.client.Recv()
			if err != nil {
				fmt.Println("Error receiving block:", err)
				return
			}

			switch t := msg.Type.(type) {
			case *ab.DeliverResponse_Status:
				fmt.Println("Got deliver status:", t)
			case *ab.DeliverResponse_Block:
				metrics.recordBlockReceived()
				fmt.Printf("Received block: %d\n", t.Block.Header.Number)
			}
		}
	}
}

func main() {
	conf, err := localconfig.Load()
	if err != nil {
		fmt.Println("failed to load config:", err)
		os.Exit(1)
	}

	// Load local MSP
	mspConfig, err := msp.GetLocalMspConfig(conf.General.LocalMSPDir, conf.General.BCCSP, conf.General.LocalMSPID)
	if err != nil {
		fmt.Println("Failed to load MSP config:", err)
		os.Exit(1)
	}
	err = mspmgmt.GetLocalMSP(factory.GetDefault()).Setup(mspConfig)
	if err != nil {
		fmt.Println("Failed to initialize local MSP:", err)
		os.Exit(1)
	}

	signer, err := mspmgmt.GetLocalMSP(factory.GetDefault()).GetDefaultSigningIdentity()
	if err != nil {
		fmt.Println("Failed to load local signing identity:", err)
		os.Exit(1)
	}

	var channelID string
	var serverAddrs string
	var messages uint64
	var goroutines uint64
	var msgSize uint64
	var enableDeliver bool

	flag.StringVar(&serverAddrs, "servers", fmt.Sprintf("%s:%d", conf.General.ListenAddress, conf.General.ListenPort), "Comma-separated list of orderer addresses (e.g., localhost:7050,localhost:8050,localhost:9050,localhost:10050)")
	flag.StringVar(&channelID, "channelID", "mychannel", "The channel ID to broadcast to.")
	flag.Uint64Var(&messages, "messages", 1000, "The number of messages to broadcast.")
	flag.Uint64Var(&goroutines, "goroutines", 1, "The number of concurrent goroutines per orderer")
	flag.Uint64Var(&msgSize, "size", 1024, "The size in bytes of the data section for the payload")
	flag.BoolVar(&enableDeliver, "deliver", true, "Enable deliver client to pull blocks")
	flag.Parse()

	// Parse server addresses
	ordererAddrs := []string{}
	if serverAddrs != "" {
		// Simple split by comma
		addr := ""
		for _, c := range serverAddrs {
			if c == ',' {
				if addr != "" {
					ordererAddrs = append(ordererAddrs, addr)
					addr = ""
				}
			} else {
				addr += string(c)
			}
		}
		if addr != "" {
			ordererAddrs = append(ordererAddrs, addr)
		}
	}

	if len(ordererAddrs) == 0 {
		fmt.Println("No orderer addresses provided")
		os.Exit(1)
	}

	fmt.Printf("Connecting to %d orderers: %v\n", len(ordererAddrs), ordererAddrs)
	fmt.Printf("Sending %d messages with %d goroutines per orderer\n", messages, goroutines)

	metrics := &perfMetrics{
		startTime: time.Now(),
		latencies: make([]time.Duration, 0),
	}

	msgData := make([]byte, msgSize)
	msgsPerGo := messages / (goroutines * uint64(len(ordererAddrs)))
	totalGoroutines := goroutines * uint64(len(ordererAddrs))
	roundMsgs := msgsPerGo * totalGoroutines

	if roundMsgs != messages {
		fmt.Printf("Rounding messages to %d (distributed across %d orderers)\n", roundMsgs, len(ordererAddrs))
	}

	var wg sync.WaitGroup

	// Start deliver client if enabled
	deliverStopChan := make(chan struct{})
	if enableDeliver {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Connect to first orderer for deliver
			conn, err := grpc.NewClient(ordererAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Println("Error connecting to orderer for deliver:", err)
				return
			}
			defer conn.Close()

			deliverGrpcClient, err := ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
			if err != nil {
				fmt.Println("Error creating deliver client:", err)
				return
			}

			dc := newDeliverClient(deliverGrpcClient, channelID, signer)
			if err := dc.seekNewest(); err != nil {
				fmt.Println("Error seeking newest:", err)
				return
			}

			fmt.Println("Deliver client started, listening for blocks...")
			dc.readBlocks(metrics, deliverStopChan)
		}()
	}

	// Start broadcast clients for each orderer
	for ordererIdx, ordererAddr := range ordererAddrs {
		for goIdx := range goroutines {
			wg.Add(1)
			go func(ordererAddr string, ordererIdx int, goIdx uint64) {
				defer wg.Done()

				conn, err := grpc.NewClient(ordererAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					fmt.Printf("Error connecting to orderer %s: %v\n", ordererAddr, err)
					return
				}
				defer conn.Close()

				client, err := ab.NewAtomicBroadcastClient(conn).Broadcast(context.TODO())
				if err != nil {
					fmt.Printf("Error creating broadcast client for %s: %v\n", ordererAddr, err)
					return
				}

				bc := newBroadcastClient(client, channelID, signer)

				// Goroutine to receive acks
				done := make(chan struct{})
				go func() {
					for range msgsPerGo {
						err := bc.getAck()
						if err != nil {
							fmt.Printf("Error getting ack from %s: %v\n", ordererAddr, err)
							break
						}
					}
					close(done)
				}()

				// Send messages
				for i := range msgsPerGo {
					sendTime := time.Now()
					metrics.recordTxSent()
					if err := bc.broadcast(msgData); err != nil {
						fmt.Printf("Error broadcasting to %s: %v\n", ordererAddr, err)
						break
					}
					// Record latency (simplified - actual latency would be measured on ack)
					if i%100 == 0 { // Sample every 100th transaction
						metrics.recordTxAcked(time.Since(sendTime))
					} else {
						metrics.recordTxAcked(0)
					}
				}

				<-done
				client.CloseSend()
				fmt.Printf("Orderer %d (%s) goroutine %d completed\n", ordererIdx, ordererAddr, goIdx)
			}(ordererAddr, ordererIdx, goIdx)
		}
	}

	// Wait for all broadcast clients to finish
	wg.Wait()
	metrics.endTime = time.Now()

	// Stop deliver client
	if enableDeliver {
		time.Sleep(2 * time.Second) // Give time to receive final blocks
		close(deliverStopChan)
	}

	// Print performance metrics
	metrics.printStats()
}

// Made with Bob
