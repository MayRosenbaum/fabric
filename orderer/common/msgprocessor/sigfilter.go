/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgprocessor

import (
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/orderer/txservice"
)

// SigFilterSupport provides the resources required for the signature filter
type SigFilterSupport interface {
	// PolicyManager returns a reference to the current policy manager
	PolicyManager() policies.Manager
	// OrdererConfig returns the config.Orderer for the channel and whether the Orderer config exists
	OrdererConfig() (channelconfig.Orderer, bool)
}

// SigFilter stores the name of the policy to apply to deliver requests to
// determine whether a client is authorized
type SigFilter struct {
	normalPolicyName      string
	maintenancePolicyName string
	support               SigFilterSupport
}

// NewSigFilter creates a new signature filter, at every evaluation, the policy manager is called
// to retrieve the latest version of the policy.
//
// normalPolicyName is applied when Orderer/ConsensusType.State = NORMAL
// maintenancePolicyName is applied when Orderer/ConsensusType.State = MAINTENANCE
func NewSigFilter(normalPolicyName, maintenancePolicyName string, support SigFilterSupport) *SigFilter {
	return &SigFilter{
		normalPolicyName:      normalPolicyName,
		maintenancePolicyName: maintenancePolicyName,
		support:               support,
	}
}

//// Apply applies the policy given, resulting in Reject or Forward, never Accept
//func (sf *SigFilter) Apply(message *cb.Envelope) error {
//	// build an envelope that pass the verification
//	//privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
//	//sID := &mspprotos.SerializedIdentity{
//	//	Mspid:   "SampleOrg",
//	//	IdBytes: []byte("FAKE_CERT_BYTES"),
//	//}
//	//creatorBytes, _ := proto.Marshal(sID)
//	//payload := make([]byte, 300)
//	//for i := range payload {
//	//	payload[i] = byte(i % 255)
//	//}
//	//hash := sha256.Sum256(payload)
//	//r, s, _ := ecdsa.Sign(rand.Reader, privKey, hash[:])
//	//signature := append(r.Bytes(), s.Bytes()...)
//	//env := &cb.Envelope{
//	//	Payload:   payload,
//	//	Signature: signature,
//	//}
//
//	ordererConf, ok := sf.support.OrdererConfig()
//	if !ok {
//		logger.Panic("Programming error: orderer config not found")
//	}
//
//	signedData, err := protoutil.EnvelopeAsSignedData(env)
//	if err != nil {
//		return fmt.Errorf("could not convert message to signedData: %s", err)
//	}
//
//	// In maintenance mode, we typically require the signature of /Channel/Orderer/Writers.
//	// This will filter out configuration changes that are not related to consensus-type migration
//	// (e.g on /Channel/Application), and will block Deliver requests from peers (which are normally /Channel/Readers).
//	policyName := sf.normalPolicyName
//	if ordererConf.ConsensusState() == orderer.ConsensusType_STATE_MAINTENANCE {
//		policyName = sf.maintenancePolicyName
//	}
//
//	policy, ok := sf.support.PolicyManager().GetPolicy(policyName)
//	if !ok {
//		return fmt.Errorf("could not find policy %s", policyName)
//	}
//
//	err = policy.EvaluateSignedData(signedData)
//	if err != nil {
//		logger.Warnw("SigFilter evaluation failed", "error", err.Error(), "ConsensusState", ordererConf.ConsensusState(), "policyName", policyName, "signingIdentity", protoutil.LogMessageForSerializedIdentities(signedData))
//		return errors.Wrap(errors.WithStack(ErrPermissionDenied), err.Error())
//	}
//	return nil
//}

// Apply resulting in Accept every message, used for fabric performance
func (sf *SigFilter) Apply(message *cb.Envelope) error {
	// simulate the verification of a signature
	if len(message.Payload) <= 1000 {
		// call service300
		//logger.Info("Emulation: verify tx with size 300")
		//for i := 0; i < 10; i++ {
		isValid := txservice.TxService300.VerifyTransaction()
		if !isValid {
			logger.Warn("emulation: verify non valid tx")
		}
		//}
	} else {
		// call service3500
		//logger.Info("Emulation: verify tx with size 3500")
		//for i := 0; i < 10; i++ {
		isValid := txservice.TxService3500.VerifyTransaction()
		if !isValid {
			logger.Warn("emulation: verify non valid tx")
		}
		//}
	}
	return nil
}
