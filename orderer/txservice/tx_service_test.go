package txservice

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSignedTransactionService(t *testing.T) {
	numOfTxs := 100
	txSize := 300
	service, err := NewSignedTransactionService(numOfTxs, txSize)
	require.NoError(t, err)

	// verify random tx
	isValid := service.VerifyTransaction()
	require.True(t, isValid)
}
