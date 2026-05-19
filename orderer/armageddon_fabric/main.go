package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
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

	flag.StringVar(&servers, "servers", "127.0.0.1:7050", "Comma-separated list of orderer addresses")
	flag.StringVar(&channelID, "channelID", "mychannel", "The channel ID to broadcast to and deliver from")
	flag.IntVar(&transactions, "transactions", 1000, "The number of transactions to send")
	flag.StringVar(&rate, "rate", "500", "The number of transactions per second to send; supports one or more space-separated values")
	flag.IntVar(&txSize, "txSize", 512, "The transaction payload size in bytes")
	flag.IntVar(&expectedTxs, "expectedTxs", -1, "The expected number of transactions to receive before stopping")
	flag.StringVar(&outputDir, "output", ".", "The output directory in which to place statistics.csv")
	flag.IntVar(&pullFrom, "pullFrom", 1, "The 1-based orderer index to pull blocks from")
	flag.Parse()

	serverList := parseServers(servers)
	if len(serverList) == 0 {
		fmt.Fprintln(os.Stderr, "no orderer servers were provided")
		os.Exit(1)
	}

	if expectedTxs < 0 {
		expectedTxs = transactions
	}

	signer, err := loadLocalSigner()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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
		Signer:       signer,
	}

	loadErrCh := make(chan error, 1)
	receiveErrCh := make(chan error, 1)

	go func() {
		loadErrCh <- Load(cfg)
	}()

	go func() {
		receiveErrCh <- Receive(cfg)
	}()

	// First, wait for Load.
	if err := <-loadErrCh; err != nil {
		fmt.Fprintln(os.Stderr, "Load failed:", err)
		os.Exit(1)
	}

	fmt.Println("Load finished successfully, waiting for receiver...")

	// Then wait for Receive to finish expectedTxs.
	if err := <-receiveErrCh; err != nil {
		fmt.Fprintln(os.Stderr, "Receive failed:", err)
		os.Exit(1)
	}

	fmt.Printf("Completed successfully. Statistics written to %s/statistics.csv\n", outputDir)
}

// Made with Bob
