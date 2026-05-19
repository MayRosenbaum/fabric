package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type Config struct {
	Servers      []string
	ChannelID    string
	Transactions int
	Rate         string
	TxSize       int
	ExpectedTxs  int
	OutputDir    string
	PullFrom     int
}

type StreamInfo struct {
	conn                  *grpc.ClientConn
	stream                ab.AtomicBroadcast_BroadcastClient
	stopChan              chan struct{}
	isBroken              bool
	isAlreadyReconnecting bool
	maxRetryDelay         time.Duration
	endpoint              string
	lock                  sync.Mutex
	sentTxs               uint64
}

func (streamInfo *StreamInfo) IsBroken() bool {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	return streamInfo.isBroken
}

func (streamInfo *StreamInfo) SetIsBroken(value bool) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	streamInfo.isBroken = value
}

func (streamInfo *StreamInfo) CheckIfReconnectionIsNeeded() bool {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	if streamInfo.isAlreadyReconnecting {
		return false
	}
	streamInfo.isAlreadyReconnecting = true
	return true
}

func (streamInfo *StreamInfo) SetNewConnAndStream(newConnection *grpc.ClientConn, newStream ab.AtomicBroadcast_BroadcastClient) {
	streamInfo.lock.Lock()
	defer streamInfo.lock.Unlock()
	_ = streamInfo.conn.Close()
	streamInfo.stream = newStream
	streamInfo.conn = newConnection
	streamInfo.isBroken = false
	streamInfo.isAlreadyReconnecting = false
}

func (streamInfo *StreamInfo) TryReconnect() {
	if !streamInfo.CheckIfReconnectionIsNeeded() {
		return
	}

	go func() {
		delay := 2 * time.Second
		ticker := time.NewTicker(delay)
		defer ticker.Stop()

		for {
			select {
			case <-streamInfo.stopChan:
				return
			case <-ticker.C:
				newConn, newStream, err := createBroadcastConnAndStream(streamInfo.endpoint)
				if err != nil {
					delay *= 2
					if delay > streamInfo.maxRetryDelay {
						delay = streamInfo.maxRetryDelay
					}
					continue
				}

				streamInfo.SetNewConnAndStream(newConn, newStream)
				go receiveResponseFromOrderer(streamInfo)
				return
			}
		}
	}()
}

type BroadcastTxClient struct {
	ordererEndpoints []string
	streams          []*StreamInfo
	stopChan         chan struct{}
}

func NewBroadcastTxClient(ordererEndpoints []string) *BroadcastTxClient {
	return &BroadcastTxClient{
		ordererEndpoints: ordererEndpoints,
		streams:          make([]*StreamInfo, len(ordererEndpoints)),
		stopChan:         make(chan struct{}),
	}
}

func (c *BroadcastTxClient) InitStreams() error {
	for i, ordererEndpoint := range c.ordererEndpoints {
		conn, stream, err := createBroadcastConnAndStream(ordererEndpoint)
		if err != nil {
			return err
		}

		c.streams[i] = &StreamInfo{
			conn:          conn,
			stream:        stream,
			stopChan:      make(chan struct{}),
			maxRetryDelay: 8 * time.Second,
			endpoint:      ordererEndpoint,
		}
	}

	return nil
}

func (c *BroadcastTxClient) SendTxToAllOrderers(envelope *cb.Envelope) {
	for _, streamInfo := range c.streams {
		if !streamInfo.IsBroken() {
			err := streamInfo.stream.Send(envelope)
			if err != nil {
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect()
			} else {
				atomic.AddUint64(&streamInfo.sentTxs, 1)
			}
		}
	}
}

func (c *BroadcastTxClient) Stop() error {
	for _, streamInfo := range c.streams {
		if err := streamInfo.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection to orderer %s", streamInfo.endpoint)
		}
		close(streamInfo.stopChan)
	}
	close(c.stopChan)
	return nil
}

func createBroadcastConnAndStream(endpoint string) (*grpc.ClientConn, ab.AtomicBroadcast_BroadcastClient, error) {
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection to orderer %s: %v", endpoint, err)
	}

	stream, err := ab.NewAtomicBroadcastClient(conn).Broadcast(context.TODO())
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to open broadcast stream to orderer %s: %v", endpoint, err)
	}

	return conn, stream, nil
}

func receiveResponseFromOrderer(streamInfo *StreamInfo) {
	for {
		select {
		case <-streamInfo.stopChan:
			return
		default:
			_, err := streamInfo.stream.Recv()
			if err != nil {
				streamInfo.SetIsBroken(true)
				streamInfo.TryReconnect()
				return
			}
		}
	}
}

type RateLimiter struct {
	tokens       float64
	capacity     int
	fillInterval time.Duration
	fillQuota    float64
	mutex        sync.Mutex
	cond         sync.Cond
	stopChan     chan bool
	stopped      bool
	wg           sync.WaitGroup
}

func NewRateLimiter(rateLimit int, fillInterval time.Duration, capacity int) (*RateLimiter, error) {
	fillQuota := float64(rateLimit*int(fillInterval.Milliseconds())) / 1000.0

	if capacity <= 0 {
		return nil, fmt.Errorf("invalid capacity: (%d)", capacity)
	}

	if fillQuota <= 0.0 {
		return nil, fmt.Errorf("invalid fillQuota: (%.2f)", fillQuota)
	}

	if fillQuota > float64(capacity) {
		return nil, fmt.Errorf("fillQuota (%.2f) must be less than or equal to capacity (%d)", fillQuota, capacity)
	}

	rl := &RateLimiter{
		tokens:       float64(capacity),
		capacity:     capacity,
		fillInterval: fillInterval,
		fillQuota:    fillQuota,
		stopChan:     make(chan bool),
		stopped:      false,
	}
	rl.cond = sync.Cond{L: &rl.mutex}

	rl.wg.Add(1)
	go rl.fillBucket()
	return rl, nil
}

func (rl *RateLimiter) fillBucket() {
	defer rl.wg.Done()

	ticker := time.NewTicker(rl.fillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mutex.Lock()
			rl.tokens += rl.fillQuota
			if rl.tokens > float64(rl.capacity) {
				rl.tokens = float64(rl.capacity)
			}
			rl.cond.Broadcast()
			rl.mutex.Unlock()
		case <-rl.stopChan:
			rl.mutex.Lock()
			rl.cond.Broadcast()
			rl.mutex.Unlock()
			return
		}
	}
}

func (rl *RateLimiter) Stop() {
	rl.mutex.Lock()
	if rl.stopped {
		rl.mutex.Unlock()
		return
	}
	rl.stopped = true
	close(rl.stopChan)
	rl.cond.Broadcast()
	rl.mutex.Unlock()
	rl.wg.Wait()
}

func (rl *RateLimiter) GetToken() bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	for {
		if rl.stopped {
			return false
		}

		if rl.tokens >= 1.0 {
			rl.tokens--
			return true
		}

		rl.cond.Wait()
	}
}

type Statistics struct {
	timeStamp     float64
	numOfTxs      int
	numOfBlocks   int
	sumOfTxsDelay float64
	sumOfTxsSize  int
}

type StatisticsAggregator struct {
	mu        sync.Mutex
	startTime int64
	statistic Statistics
}

func (sta *StatisticsAggregator) Add(numOfTxs int, numOfBlocks int, sumOfTxsDelay float64, sumOfTxsSize int) {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	sta.statistic.numOfTxs += numOfTxs
	sta.statistic.numOfBlocks += numOfBlocks
	sta.statistic.sumOfTxsDelay += sumOfTxsDelay
	sta.statistic.sumOfTxsSize += sumOfTxsSize
}

func (sta *StatisticsAggregator) ReadAndReset() Statistics {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	currentTime := time.Now().UnixMilli()
	timeSinceStartMs := currentTime - sta.startTime
	timeSinceStartS := float64(timeSinceStartMs) / 1000
	val := Statistics{
		timeStamp:     timeSinceStartS,
		numOfTxs:      sta.statistic.numOfTxs,
		numOfBlocks:   sta.statistic.numOfBlocks,
		sumOfTxsDelay: sta.statistic.sumOfTxsDelay,
		sumOfTxsSize:  sta.statistic.sumOfTxsSize,
	}
	sta.statistic.numOfTxs = 0
	sta.statistic.numOfBlocks = 0
	sta.statistic.sumOfTxsDelay = 0.0
	sta.statistic.sumOfTxsSize = 0
	return val
}

type BlockWithTime struct {
	block        *cb.Block
	acceptedTime time.Time
}

func Load(cfg Config) error {
	rates := strings.Fields(cfg.Rate)
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("no orderer servers were provided")
	}

	txMinimumSize := 16 + 8 + 8
	if cfg.TxSize < txMinimumSize {
		return fmt.Errorf("the required tx size: %d is less than the minimum size: %d", cfg.TxSize, txMinimumSize)
	}

	signer, err := loadLocalSigner()
	if err != nil {
		return err
	}

	convertedRates := make([]int, len(rates))
	for i := 0; i < len(rates); i++ {
		convertedRates[i], err = strconv.Atoi(rates[i])
		if err != nil {
			return fmt.Errorf("rate is not valid: %w", err)
		}
	}

	for _, convertedRate := range convertedRates {
		start := time.Now()
		err := sendTxsToAllAvailableOrderers(cfg.Servers, cfg.ChannelID, signer, cfg.Transactions, convertedRate, cfg.TxSize)
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		reportLoadResults(cfg.Transactions, elapsed, cfg.TxSize)
	}

	return nil
}

func sendTxsToAllAvailableOrderers(ordererEndpoints []string, channelID string, signer identity.SignerSerializer, numOfTxs int, rate int, txSize int) error {
	broadcastClient := NewBroadcastTxClient(ordererEndpoints)
	err := broadcastClient.InitStreams()
	if err != nil {
		return fmt.Errorf("failed to init streams between client and orderers: %w", err)
	}

	sessionNumber := make([]byte, 16)
	_, err = rand.Read(sessionNumber)
	if err != nil {
		return fmt.Errorf("failed to create a session number: %w", err)
	}

	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	capacity := rate / fillFrequency
	rl, err := NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		return fmt.Errorf("failed to start a rate limiter: %w", err)
	}

	for _, streamInfo := range broadcastClient.streams {
		go receiveResponseFromOrderer(streamInfo)
	}

	for i := 0; i < numOfTxs; i++ {
		env, err := createFabricBroadcastEnvelope(i, txSize, sessionNumber, channelID, signer)
		if err != nil {
			return err
		}

		if !rl.GetToken() {
			return fmt.Errorf("failed to send tx %d", i+1)
		}

		broadcastClient.SendTxToAllOrderers(env)
	}

	rl.Stop()
	return broadcastClient.Stop()
}

func Receive(cfg Config) error {
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("no orderer servers were provided")
	}

	if cfg.PullFrom <= 0 || cfg.PullFrom > len(cfg.Servers) {
		return fmt.Errorf("pullFrom %d is out of range, number of orderers: %d", cfg.PullFrom, len(cfg.Servers))
	}

	signer, err := loadLocalSigner()
	if err != nil {
		return err
	}

	return pullBlocksFromOrdererAndCollectStatistics(cfg.Servers, cfg.ChannelID, signer, cfg.PullFrom, cfg.OutputDir, cfg.ExpectedTxs)
}

func pullBlocksFromOrdererAndCollectStatistics(ordererEndpoints []string, channelID string, signer identity.SignerSerializer, pullFrom int, receiveOutputDir string, expectedNumOfTxs int) error {
	requestEnvelope, err := createDeliverEnvelope(channelID, signer)
	if err != nil {
		return fmt.Errorf("failed to create a request envelope: %w", err)
	}

	endpointToPullFrom := ordererEndpoints[pullFrom-1]
	conn, err := grpc.NewClient(
		endpointToPullFrom,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024)),
	)
	if err != nil {
		return fmt.Errorf("failed to create a gRPC client connection to orderer %d: %w", pullFrom, err)
	}
	defer conn.Close()

	stream, err := ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to create a deliver stream to orderer %d: %w", pullFrom, err)
	}

	err = stream.Send(requestEnvelope)
	if err != nil {
		return fmt.Errorf("failed to send a request envelope to orderer %d: %w", pullFrom, err)
	}

	statisticsAggregator := &StatisticsAggregator{}
	statisticChan := make(chan Statistics, 60)
	blockChan := make(chan BlockWithTime)
	stopChan := make(chan bool)

	var waitToFinish sync.WaitGroup
	waitToFinish.Add(4)

	statisticsAggregator.startTime = time.Now().UnixMilli()
	startTimeS := float64(statisticsAggregator.startTime) / 1000
	timeIntervalToSampleStat := 1 * time.Second

	go func() {
		manageStatistics(receiveOutputDir, statisticChan, stopChan, startTimeS, expectedNumOfTxs, pullFrom, timeIntervalToSampleStat)
		waitToFinish.Done()
	}()

	go func() {
		ticker := time.NewTicker(timeIntervalToSampleStat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				lastStat := statisticsAggregator.ReadAndReset()
				statisticChan <- lastStat
			case <-stopChan:
				waitToFinish.Done()
				return
			}
		}
	}()

	go func() {
		var txsTotal int
		for {
			block, err := pullBlock(stream, endpointToPullFrom, conn)
			if err != nil {
				panic(err)
			}

			if block.Header.Number == 0 {
				continue
			}

			blockWithTime := BlockWithTime{
				block:        block,
				acceptedTime: time.Now(),
			}
			blockChan <- blockWithTime
			txsTotal += len(blockWithTime.block.Data.Data)

			if expectedNumOfTxs > 0 && expectedNumOfTxs <= txsTotal {
				waitToFinish.Done()
				return
			}
		}
	}()

	go func() {
		var txsTotal int
		for {
			blockWithTime := <-blockChan
			sumOfDelayTimes := 0.0
			sumOfTxsSize := 0
			txs := len(blockWithTime.block.Data.Data)
			txsTotal += txs

			for j := 0; j < txs; j++ {
				env, err := protoutil.GetEnvelopeFromBlock(blockWithTime.block.Data.Data[j])
				if err != nil {
					panic(err)
				}
				data, err := extractPayloadBytes(env)
				if err != nil {
					panic(err)
				}

				sumOfTxsSize += len(protoutil.MarshalOrPanic(env))
				delay := calculateDelayOfTx(data, blockWithTime.acceptedTime)
				sumOfDelayTimes += delay.Seconds()
			}
			statisticsAggregator.Add(txs, 1, sumOfDelayTimes, sumOfTxsSize)

			if expectedNumOfTxs > 0 && expectedNumOfTxs <= txsTotal {
				close(stopChan)
				waitToFinish.Done()
				return
			}
		}
	}()

	waitToFinish.Wait()
	return nil
}

func calculateDelayOfTx(data []byte, acceptedTime time.Time) time.Duration {
	sendTime := extractTimestampFromTx(data)
	return acceptedTime.Sub(sendTime)
}

func pullBlock(stream ab.AtomicBroadcast_DeliverClient, endpointToPullFrom string, conn *grpc.ClientConn) (*cb.Block, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive a deliver response from %s", endpointToPullFrom)
	}

	block := resp.GetBlock()
	if block == nil {
		_ = stream.CloseSend()
		_ = conn.Close()
		return nil, fmt.Errorf("received a non block message from %s: %v", endpointToPullFrom, resp)
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		_ = stream.CloseSend()
		_ = conn.Close()
		return nil, fmt.Errorf("received empty block from %s", endpointToPullFrom)
	}

	return block, nil
}

func createDeliverEnvelope(channelID string, signer identity.SignerSerializer) (*cb.Envelope, error) {
	return protoutil.CreateSignedEnvelope(
		cb.HeaderType_DELIVER_SEEK_INFO,
		channelID,
		signer,
		&ab.SeekInfo{
			Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}}},
			Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
			Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
			ErrorResponse: ab.SeekInfo_BEST_EFFORT,
		},
		0,
		0,
	)
}

func createFabricBroadcastEnvelope(index int, txSize int, sessionNumber []byte, channelID string, signer identity.SignerSerializer) (*cb.Envelope, error) {
	payload := make([]byte, txSize)
	copy(payload[:16], sessionNumber)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 8; i++ {
		payload[16+i] = byte(timestamp >> (8 * i))
		payload[24+i] = byte(index >> (8 * i))
	}

	return protoutil.CreateSignedEnvelope(
		cb.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		signer,
		&cb.Envelope{Payload: payload},
		0,
		0,
	)
}

func extractPayloadBytes(env *cb.Envelope) ([]byte, error) {
	payload := &cb.Payload{}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, err
	}

	return payload.Data, nil
}

func extractTimestampFromTx(data []byte) time.Time {
	if len(data) < 24 {
		return time.Unix(0, 0)
	}

	var ts int64
	for i := 0; i < 8; i++ {
		ts |= int64(data[16+i]) << (8 * i)
	}
	return time.Unix(0, ts)
}

func reportLoadResults(transactions int, elapsed time.Duration, txSize int) {
	avgTxSendingRate := float64(transactions) / elapsed.Seconds()
	fmt.Printf("Load command finished, sent %d TXs in %v seconds, TX size %d, avg. tx sending rate: %.2f\n", transactions, elapsed, txSize, avgTxSendingRate)
}

func manageStatistics(receiveOutputDir string, statisticChan <-chan Statistics, stopChan <-chan bool, startTime float64, expectedTxs int, pullFrom int, timeIntervalToSampleStat time.Duration) {
	filePath := path.Join(receiveOutputDir, "statistics.csv")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if fileInfo.Size() == 0 {
		writer := csv.NewWriter(file)
		desc := "Experiment Description: " + fmt.Sprintf("Time: %.2fs, ", startTime) + fmt.Sprintf("Receiver from orderer%d, ", pullFrom)
		if expectedTxs >= 0 {
			desc = desc + fmt.Sprintf("Expected number of txs: %d", expectedTxs)
		}

		_ = writer.Write([]string{desc, "", "", "", "", "", "", "", ""})
		_ = writer.Write([]string{""})
		_ = writer.Write([]string{"Time Since Start (s)", "Number of txs", "Number of blocks", "Avg. tx rate", "Sum of txs size", "Avg. tx size (byte)", "Sum of txs delay (s)", "Avg. tx delay (s)", "Avg. block rate", "Avg. block size (byte)", "Avg. number of txs in block"})
		writer.Flush()
	}

	for {
		select {
		case statistic := <-statisticChan:
			writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)
		case <-stopChan:
			for {
				select {
				case statistic := <-statisticChan:
					writeStatisticsToCSV(file, statistic, timeIntervalToSampleStat)
				default:
					return
				}
			}
		}
	}
}

func writeStatisticsToCSV(file *os.File, statistic Statistics, timeIntervalToSampleStat time.Duration) {
	writer := csv.NewWriter(file)
	defer writer.Flush()
	defer file.Sync()

	var avgTxSize int
	var avgTxDelay float64
	var avgBlockSize int
	var avgNumOfTxsInBlock int
	if statistic.numOfTxs != 0 {
		avgTxSize = statistic.sumOfTxsSize / statistic.numOfTxs
		avgTxDelay = statistic.sumOfTxsDelay / float64(statistic.numOfTxs)
		avgBlockSize = statistic.sumOfTxsSize / statistic.numOfBlocks
		avgNumOfTxsInBlock = statistic.numOfTxs / statistic.numOfBlocks
	}

	_ = writer.Write([]string{
		fmt.Sprintf("%.f", statistic.timeStamp),
		fmt.Sprintf("%d", statistic.numOfTxs),
		fmt.Sprintf("%d", statistic.numOfBlocks),
		fmt.Sprintf("%.2f", float64(statistic.numOfTxs)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", statistic.sumOfTxsSize),
		fmt.Sprintf("%d", avgTxSize),
		fmt.Sprintf("%.2f", statistic.sumOfTxsDelay),
		fmt.Sprintf("%.2f", avgTxDelay),
		fmt.Sprintf("%.2f", float64(statistic.numOfBlocks)/timeIntervalToSampleStat.Seconds()),
		fmt.Sprintf("%d", avgBlockSize),
		fmt.Sprintf("%d", avgNumOfTxsInBlock),
	})
}

func loadLocalSigner() (identity.SignerSerializer, error) {
	conf, err := localconfig.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	mspConfig, err := msp.GetLocalMspConfig(conf.General.LocalMSPDir, conf.General.BCCSP, conf.General.LocalMSPID)
	if err != nil {
		return nil, fmt.Errorf("failed to load MSP config: %w", err)
	}

	err = mspmgmt.GetLocalMSP(factory.GetDefault()).Setup(mspConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize local MSP: %w", err)
	}

	signer, err := mspmgmt.GetLocalMSP(factory.GetDefault()).GetDefaultSigningIdentity()
	if err != nil {
		return nil, fmt.Errorf("failed to load local signing identity: %w", err)
	}

	return signer, nil
}

// Made with Bob
