package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

func parseServers(servers string) []string {
	if servers == "" {
		return nil
	}

	parts := strings.Split(servers, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func main() {
	var servers string
	var channelID string
	var transactions int
	var rate string
	var txSize int
	var expectedTxs int
	var outputDir string
	var pullFrom int
	var receiveFirstDelay time.Duration

	flag.StringVar(&servers, "servers", "127.0.0.1:7050", "Comma-separated list of orderer addresses")
	flag.StringVar(&channelID, "channelID", "mychannel", "The channel ID to broadcast to and deliver from")
	flag.IntVar(&transactions, "transactions", 1000, "The number of transactions to send")
	flag.StringVar(&rate, "rate", "500", "The number of transactions per second to send; supports one or more space-separated values")
	flag.IntVar(&txSize, "txSize", 512, "The transaction payload size in bytes")
	flag.IntVar(&expectedTxs, "expectedTxs", -1, "The expected number of transactions to receive before stopping")
	flag.StringVar(&outputDir, "output", ".", "The output directory in which to place statistics.csv")
	flag.IntVar(&pullFrom, "pullFrom", 1, "The 1-based orderer index to pull blocks from")
	flag.DurationVar(&receiveFirstDelay, "receiveFirstDelay", 2*time.Second, "How long to wait after starting receive before starting load")
	flag.Parse()

	serverList := parseServers(servers)
	if len(serverList) == 0 {
		fmt.Fprintln(os.Stderr, "no orderer servers were provided")
		os.Exit(1)
	}

	if expectedTxs < 0 {
		expectedTxs = transactions
	}

	cfg := Config{
		Servers:      serverList,
		ChannelID:    channelID,
		Transactions: transactions,
		Rate:         rate,
		TxSize:       txSize,
		ExpectedTxs:  expectedTxs,
		OutputDir:    outputDir,
		PullFrom:     pullFrom,
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- Receive(cfg)
	}()

	time.Sleep(receiveFirstDelay)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- Load(cfg)
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Completed successfully. Statistics written to %s/statistics.csv\n", outputDir)
}

// Made with Bob
