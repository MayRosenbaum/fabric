package txservice

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"github.com/hyperledger/fabric-lib-go/bccsp/utils"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/cryptogen/ca"
	"math/big"
	"os"
	"time"
)

var TxService300 *SignedTransactionService
var TxService3500 *SignedTransactionService

type ECDSASigner ecdsa.PrivateKey

func (s ECDSASigner) Sign(message []byte) ([]byte, error) {
	digest := util.ComputeSHA256(message)
	sk := ecdsa.PrivateKey(s)
	return signECDSA(&sk, digest)
}

// TODO: implement correct Serialize
// Serialize is called when a SignatureHeader.Creator is created. Since this creator is placeholder, the SignatureHeader.Creator must be updated with correct creator.
func (s ECDSASigner) Serialize() ([]byte, error) {
	return []byte("creator"), nil
}

func signECDSA(k *ecdsa.PrivateKey, digest []byte) (signature []byte, err error) {
	r, s, err := ecdsa.Sign(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}

	s, err = utils.ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}

	return marshalECDSASignature(r, s)
}

func marshalECDSASignature(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(ECDSASignature{r, s})
}

type ECDSASignature struct {
	R, S *big.Int
}

// SignedTransactionService holds signed transactions with the cryptographic keys.
type SignedTransactionService struct {
	txs         []*common.Envelope
	privateKey  *ecdsa.PrivateKey
	certificate *x509.Certificate
}

func NewSignedTransactionService(numOfTxs int, txSize int) (*SignedTransactionService, error) {
	// Create private key and certificate used to sign and verify txs
	pk, cert, err := createSignerPKAndCert()
	if err != nil {
		return nil, err
	}

	// Create signed transactions
	txs, err := createSignedTransactions(numOfTxs, txSize, (*ECDSASigner)(pk))
	if err != nil {
		return nil, err
	}

	service := &SignedTransactionService{
		txs:         txs,
		privateKey:  pk,
		certificate: cert,
	}

	return service, nil
}

// GetRandomTransactionIndex returns a random number in [0, len(txs)).
func (sts *SignedTransactionService) getRandomTransactionIndex() (int, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(sts.txs))))
	return int(n.Int64()), err
}

// VerifyTransaction verifies the transaction[txIndex] in the transactions list.
func (sts *SignedTransactionService) VerifyTransaction() bool {
	idx, _ := sts.getRandomTransactionIndex()
	// Get the transaction from the list
	envelope := sts.txs[idx]
	digest := sha256.Sum256(envelope.Payload)

	// Extract public key from the cert
	publicKey := sts.certificate.PublicKey.(*ecdsa.PublicKey)

	// Verify the signature over the digest with the public key
	valid := ecdsa.VerifyASN1(publicKey, digest[:], envelope.Signature)
	return valid
}

// createSignerPKAndCert create a CA, private key and a certificate.
// NOTE: this function is based on Fabric internal methods.
func createSignerPKAndCert() (*ecdsa.PrivateKey, *x509.Certificate, error) {
	// Create a fake CA
	dir, err := os.MkdirTemp("", "ca")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a temp dir, err: %s", err)
	}
	defer os.RemoveAll(dir)

	signCA, err := ca.NewCA(dir, "signCA", "ca", "US", "California", "San Francisco", "ARMA", "addr", "12345", "ecdsa")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a fake CA, err: %s", err)
	}

	// Create private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a private key, err: %s", err)
	}

	// Issue a certificate with the public key associated to the generated private key (the certificate contains the public key)
	cert, err := signCA.SignCertificate(dir, "signer", nil, nil, getPublicKey(privateKey), x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create a certificate, err: %s", err)
	}

	return privateKey, cert, err
}

func createSignedTransactions(numOfTxs int, txSize int, signer *ECDSASigner) ([]*common.Envelope, error) {
	sessionNumber := make([]byte, 16)
	_, err := rand.Read(sessionNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to create a session number, err: %s", err)
	}

	txs := make([]*common.Envelope, numOfTxs)
	for i := 0; i < numOfTxs; i++ {
		payload := createPayload(i, txSize, sessionNumber)
		signedTx, err := signTransaction(payload, signer)
		if err != nil {
			return nil, fmt.Errorf("failed to sign transaction %d", i)
		}
		txs[i] = signedTx
	}

	return txs, nil
}

// prepareTx is used only in performance testing and its content consists of the tx number, the time stamp (creation time) and the session number the tx is sent through
func prepareTx(txNumber int, requiredDataSize int, sessionNumber []byte) []byte {
	// create timestamp (8 bytes)
	timeStamp := uint64(time.Now().UnixNano())

	// prepare the payload data
	dataInfoSize := 8 + 8 + len(sessionNumber) // size of info fields stored in the data
	buffer := make([]byte, max(requiredDataSize, dataInfoSize))
	buff := bytes.NewBuffer(buffer[:0])
	binary.Write(buff, binary.BigEndian, uint64(txNumber))
	binary.Write(buff, binary.BigEndian, timeStamp)
	buff.Write(sessionNumber)
	result := buff.Bytes()
	if len(buff.Bytes()) < requiredDataSize {
		padding := make([]byte, requiredDataSize-len(result))
		result = append(result, padding...)
	}
	return result
}

func createPayload(txNum int, txSize int, sessionNumber []byte) []byte {
	payload := prepareTx(txNum, txSize, sessionNumber)
	return payload
}

func signTransaction(payload []byte, signer *ECDSASigner) (*common.Envelope, error) {
	signature, err := signer.Sign(payload)
	if err != nil {
		return nil, err
	}

	envelope := &common.Envelope{
		Payload:   payload,
		Signature: signature,
	}
	return envelope, nil
}

func getPublicKey(priv crypto.PrivateKey) crypto.PublicKey {
	switch kk := priv.(type) {
	case *ecdsa.PrivateKey:
		return &kk.PublicKey
	case ed25519.PrivateKey:
		return kk.Public()
	default:
		panic("unsupported key algorithm")
	}
}
