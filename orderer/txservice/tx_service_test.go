package txservice

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSignedTransactionService(t *testing.T) {
	numOfTxs := 1000
	txSize := 300
	service, err := NewSignedTransactionService(numOfTxs, txSize)
	require.NoError(t, err)

	//verify random tx
	stime := time.Now()
	for i := 0; i < 10000; i++ {
		isValid := service.VerifyTransaction()
		require.True(t, isValid)
	}
	endtime := time.Since(stime)
	fmt.Printf("total time: %v\n", endtime)
}
