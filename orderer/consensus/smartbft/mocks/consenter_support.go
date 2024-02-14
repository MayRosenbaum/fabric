// Code generated by mockery v2.40.1. DO NOT EDIT.

package mocks

import (
	channelconfig "github.com/hyperledger/fabric/common/channelconfig"
	blockcutter "github.com/hyperledger/fabric/orderer/common/blockcutter"

	common "github.com/hyperledger/fabric-protos-go/common"

	mock "github.com/stretchr/testify/mock"

	msgprocessor "github.com/hyperledger/fabric/orderer/common/msgprocessor"

	protoutil "github.com/hyperledger/fabric/protoutil"
)

// ConsenterSupport is an autogenerated mock type for the ConsenterSupport type
type ConsenterSupport struct {
	mock.Mock
}

type ConsenterSupport_Expecter struct {
	mock *mock.Mock
}

func (_m *ConsenterSupport) EXPECT() *ConsenterSupport_Expecter {
	return &ConsenterSupport_Expecter{mock: &_m.Mock}
}

// Append provides a mock function with given fields: block
func (_m *ConsenterSupport) Append(block *common.Block) error {
	ret := _m.Called(block)

	if len(ret) == 0 {
		panic("no return value specified for Append")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(*common.Block) error); ok {
		r0 = rf(block)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ConsenterSupport_Append_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Append'
type ConsenterSupport_Append_Call struct {
	*mock.Call
}

// Append is a helper method to define mock.On call
//   - block *common.Block
func (_e *ConsenterSupport_Expecter) Append(block interface{}) *ConsenterSupport_Append_Call {
	return &ConsenterSupport_Append_Call{Call: _e.mock.On("Append", block)}
}

func (_c *ConsenterSupport_Append_Call) Run(run func(block *common.Block)) *ConsenterSupport_Append_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Block))
	})
	return _c
}

func (_c *ConsenterSupport_Append_Call) Return(_a0 error) *ConsenterSupport_Append_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_Append_Call) RunAndReturn(run func(*common.Block) error) *ConsenterSupport_Append_Call {
	_c.Call.Return(run)
	return _c
}

// Block provides a mock function with given fields: number
func (_m *ConsenterSupport) Block(number uint64) *common.Block {
	ret := _m.Called(number)

	if len(ret) == 0 {
		panic("no return value specified for Block")
	}

	var r0 *common.Block
	if rf, ok := ret.Get(0).(func(uint64) *common.Block); ok {
		r0 = rf(number)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*common.Block)
		}
	}

	return r0
}

// ConsenterSupport_Block_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Block'
type ConsenterSupport_Block_Call struct {
	*mock.Call
}

// Block is a helper method to define mock.On call
//   - number uint64
func (_e *ConsenterSupport_Expecter) Block(number interface{}) *ConsenterSupport_Block_Call {
	return &ConsenterSupport_Block_Call{Call: _e.mock.On("Block", number)}
}

func (_c *ConsenterSupport_Block_Call) Run(run func(number uint64)) *ConsenterSupport_Block_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint64))
	})
	return _c
}

func (_c *ConsenterSupport_Block_Call) Return(_a0 *common.Block) *ConsenterSupport_Block_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_Block_Call) RunAndReturn(run func(uint64) *common.Block) *ConsenterSupport_Block_Call {
	_c.Call.Return(run)
	return _c
}

// BlockCutter provides a mock function with given fields:
func (_m *ConsenterSupport) BlockCutter() blockcutter.Receiver {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for BlockCutter")
	}

	var r0 blockcutter.Receiver
	if rf, ok := ret.Get(0).(func() blockcutter.Receiver); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(blockcutter.Receiver)
		}
	}

	return r0
}

// ConsenterSupport_BlockCutter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BlockCutter'
type ConsenterSupport_BlockCutter_Call struct {
	*mock.Call
}

// BlockCutter is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) BlockCutter() *ConsenterSupport_BlockCutter_Call {
	return &ConsenterSupport_BlockCutter_Call{Call: _e.mock.On("BlockCutter")}
}

func (_c *ConsenterSupport_BlockCutter_Call) Run(run func()) *ConsenterSupport_BlockCutter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_BlockCutter_Call) Return(_a0 blockcutter.Receiver) *ConsenterSupport_BlockCutter_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_BlockCutter_Call) RunAndReturn(run func() blockcutter.Receiver) *ConsenterSupport_BlockCutter_Call {
	_c.Call.Return(run)
	return _c
}

// ChannelConfig provides a mock function with given fields:
func (_m *ConsenterSupport) ChannelConfig() channelconfig.Channel {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ChannelConfig")
	}

	var r0 channelconfig.Channel
	if rf, ok := ret.Get(0).(func() channelconfig.Channel); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(channelconfig.Channel)
		}
	}

	return r0
}

// ConsenterSupport_ChannelConfig_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ChannelConfig'
type ConsenterSupport_ChannelConfig_Call struct {
	*mock.Call
}

// ChannelConfig is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) ChannelConfig() *ConsenterSupport_ChannelConfig_Call {
	return &ConsenterSupport_ChannelConfig_Call{Call: _e.mock.On("ChannelConfig")}
}

func (_c *ConsenterSupport_ChannelConfig_Call) Run(run func()) *ConsenterSupport_ChannelConfig_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_ChannelConfig_Call) Return(_a0 channelconfig.Channel) *ConsenterSupport_ChannelConfig_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_ChannelConfig_Call) RunAndReturn(run func() channelconfig.Channel) *ConsenterSupport_ChannelConfig_Call {
	_c.Call.Return(run)
	return _c
}

// ChannelID provides a mock function with given fields:
func (_m *ConsenterSupport) ChannelID() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ChannelID")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ConsenterSupport_ChannelID_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ChannelID'
type ConsenterSupport_ChannelID_Call struct {
	*mock.Call
}

// ChannelID is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) ChannelID() *ConsenterSupport_ChannelID_Call {
	return &ConsenterSupport_ChannelID_Call{Call: _e.mock.On("ChannelID")}
}

func (_c *ConsenterSupport_ChannelID_Call) Run(run func()) *ConsenterSupport_ChannelID_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_ChannelID_Call) Return(_a0 string) *ConsenterSupport_ChannelID_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_ChannelID_Call) RunAndReturn(run func() string) *ConsenterSupport_ChannelID_Call {
	_c.Call.Return(run)
	return _c
}

// ClassifyMsg provides a mock function with given fields: chdr
func (_m *ConsenterSupport) ClassifyMsg(chdr *common.ChannelHeader) msgprocessor.Classification {
	ret := _m.Called(chdr)

	if len(ret) == 0 {
		panic("no return value specified for ClassifyMsg")
	}

	var r0 msgprocessor.Classification
	if rf, ok := ret.Get(0).(func(*common.ChannelHeader) msgprocessor.Classification); ok {
		r0 = rf(chdr)
	} else {
		r0 = ret.Get(0).(msgprocessor.Classification)
	}

	return r0
}

// ConsenterSupport_ClassifyMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ClassifyMsg'
type ConsenterSupport_ClassifyMsg_Call struct {
	*mock.Call
}

// ClassifyMsg is a helper method to define mock.On call
//   - chdr *common.ChannelHeader
func (_e *ConsenterSupport_Expecter) ClassifyMsg(chdr interface{}) *ConsenterSupport_ClassifyMsg_Call {
	return &ConsenterSupport_ClassifyMsg_Call{Call: _e.mock.On("ClassifyMsg", chdr)}
}

func (_c *ConsenterSupport_ClassifyMsg_Call) Run(run func(chdr *common.ChannelHeader)) *ConsenterSupport_ClassifyMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.ChannelHeader))
	})
	return _c
}

func (_c *ConsenterSupport_ClassifyMsg_Call) Return(_a0 msgprocessor.Classification) *ConsenterSupport_ClassifyMsg_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_ClassifyMsg_Call) RunAndReturn(run func(*common.ChannelHeader) msgprocessor.Classification) *ConsenterSupport_ClassifyMsg_Call {
	_c.Call.Return(run)
	return _c
}

// CreateNextBlock provides a mock function with given fields: messages
func (_m *ConsenterSupport) CreateNextBlock(messages []*common.Envelope) *common.Block {
	ret := _m.Called(messages)

	if len(ret) == 0 {
		panic("no return value specified for CreateNextBlock")
	}

	var r0 *common.Block
	if rf, ok := ret.Get(0).(func([]*common.Envelope) *common.Block); ok {
		r0 = rf(messages)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*common.Block)
		}
	}

	return r0
}

// ConsenterSupport_CreateNextBlock_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CreateNextBlock'
type ConsenterSupport_CreateNextBlock_Call struct {
	*mock.Call
}

// CreateNextBlock is a helper method to define mock.On call
//   - messages []*common.Envelope
func (_e *ConsenterSupport_Expecter) CreateNextBlock(messages interface{}) *ConsenterSupport_CreateNextBlock_Call {
	return &ConsenterSupport_CreateNextBlock_Call{Call: _e.mock.On("CreateNextBlock", messages)}
}

func (_c *ConsenterSupport_CreateNextBlock_Call) Run(run func(messages []*common.Envelope)) *ConsenterSupport_CreateNextBlock_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]*common.Envelope))
	})
	return _c
}

func (_c *ConsenterSupport_CreateNextBlock_Call) Return(_a0 *common.Block) *ConsenterSupport_CreateNextBlock_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_CreateNextBlock_Call) RunAndReturn(run func([]*common.Envelope) *common.Block) *ConsenterSupport_CreateNextBlock_Call {
	_c.Call.Return(run)
	return _c
}

// Height provides a mock function with given fields:
func (_m *ConsenterSupport) Height() uint64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Height")
	}

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// ConsenterSupport_Height_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Height'
type ConsenterSupport_Height_Call struct {
	*mock.Call
}

// Height is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) Height() *ConsenterSupport_Height_Call {
	return &ConsenterSupport_Height_Call{Call: _e.mock.On("Height")}
}

func (_c *ConsenterSupport_Height_Call) Run(run func()) *ConsenterSupport_Height_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_Height_Call) Return(_a0 uint64) *ConsenterSupport_Height_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_Height_Call) RunAndReturn(run func() uint64) *ConsenterSupport_Height_Call {
	_c.Call.Return(run)
	return _c
}

// ProcessConfigMsg provides a mock function with given fields: env
func (_m *ConsenterSupport) ProcessConfigMsg(env *common.Envelope) (*common.Envelope, uint64, error) {
	ret := _m.Called(env)

	if len(ret) == 0 {
		panic("no return value specified for ProcessConfigMsg")
	}

	var r0 *common.Envelope
	var r1 uint64
	var r2 error
	if rf, ok := ret.Get(0).(func(*common.Envelope) (*common.Envelope, uint64, error)); ok {
		return rf(env)
	}
	if rf, ok := ret.Get(0).(func(*common.Envelope) *common.Envelope); ok {
		r0 = rf(env)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*common.Envelope)
		}
	}

	if rf, ok := ret.Get(1).(func(*common.Envelope) uint64); ok {
		r1 = rf(env)
	} else {
		r1 = ret.Get(1).(uint64)
	}

	if rf, ok := ret.Get(2).(func(*common.Envelope) error); ok {
		r2 = rf(env)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// ConsenterSupport_ProcessConfigMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ProcessConfigMsg'
type ConsenterSupport_ProcessConfigMsg_Call struct {
	*mock.Call
}

// ProcessConfigMsg is a helper method to define mock.On call
//   - env *common.Envelope
func (_e *ConsenterSupport_Expecter) ProcessConfigMsg(env interface{}) *ConsenterSupport_ProcessConfigMsg_Call {
	return &ConsenterSupport_ProcessConfigMsg_Call{Call: _e.mock.On("ProcessConfigMsg", env)}
}

func (_c *ConsenterSupport_ProcessConfigMsg_Call) Run(run func(env *common.Envelope)) *ConsenterSupport_ProcessConfigMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Envelope))
	})
	return _c
}

func (_c *ConsenterSupport_ProcessConfigMsg_Call) Return(_a0 *common.Envelope, _a1 uint64, _a2 error) *ConsenterSupport_ProcessConfigMsg_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *ConsenterSupport_ProcessConfigMsg_Call) RunAndReturn(run func(*common.Envelope) (*common.Envelope, uint64, error)) *ConsenterSupport_ProcessConfigMsg_Call {
	_c.Call.Return(run)
	return _c
}

// ProcessConfigUpdateMsg provides a mock function with given fields: env
func (_m *ConsenterSupport) ProcessConfigUpdateMsg(env *common.Envelope) (*common.Envelope, uint64, error) {
	ret := _m.Called(env)

	if len(ret) == 0 {
		panic("no return value specified for ProcessConfigUpdateMsg")
	}

	var r0 *common.Envelope
	var r1 uint64
	var r2 error
	if rf, ok := ret.Get(0).(func(*common.Envelope) (*common.Envelope, uint64, error)); ok {
		return rf(env)
	}
	if rf, ok := ret.Get(0).(func(*common.Envelope) *common.Envelope); ok {
		r0 = rf(env)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*common.Envelope)
		}
	}

	if rf, ok := ret.Get(1).(func(*common.Envelope) uint64); ok {
		r1 = rf(env)
	} else {
		r1 = ret.Get(1).(uint64)
	}

	if rf, ok := ret.Get(2).(func(*common.Envelope) error); ok {
		r2 = rf(env)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// ConsenterSupport_ProcessConfigUpdateMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ProcessConfigUpdateMsg'
type ConsenterSupport_ProcessConfigUpdateMsg_Call struct {
	*mock.Call
}

// ProcessConfigUpdateMsg is a helper method to define mock.On call
//   - env *common.Envelope
func (_e *ConsenterSupport_Expecter) ProcessConfigUpdateMsg(env interface{}) *ConsenterSupport_ProcessConfigUpdateMsg_Call {
	return &ConsenterSupport_ProcessConfigUpdateMsg_Call{Call: _e.mock.On("ProcessConfigUpdateMsg", env)}
}

func (_c *ConsenterSupport_ProcessConfigUpdateMsg_Call) Run(run func(env *common.Envelope)) *ConsenterSupport_ProcessConfigUpdateMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Envelope))
	})
	return _c
}

func (_c *ConsenterSupport_ProcessConfigUpdateMsg_Call) Return(config *common.Envelope, configSeq uint64, err error) *ConsenterSupport_ProcessConfigUpdateMsg_Call {
	_c.Call.Return(config, configSeq, err)
	return _c
}

func (_c *ConsenterSupport_ProcessConfigUpdateMsg_Call) RunAndReturn(run func(*common.Envelope) (*common.Envelope, uint64, error)) *ConsenterSupport_ProcessConfigUpdateMsg_Call {
	_c.Call.Return(run)
	return _c
}

// ProcessNormalMsg provides a mock function with given fields: env
func (_m *ConsenterSupport) ProcessNormalMsg(env *common.Envelope) (uint64, error) {
	ret := _m.Called(env)

	if len(ret) == 0 {
		panic("no return value specified for ProcessNormalMsg")
	}

	var r0 uint64
	var r1 error
	if rf, ok := ret.Get(0).(func(*common.Envelope) (uint64, error)); ok {
		return rf(env)
	}
	if rf, ok := ret.Get(0).(func(*common.Envelope) uint64); ok {
		r0 = rf(env)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	if rf, ok := ret.Get(1).(func(*common.Envelope) error); ok {
		r1 = rf(env)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ConsenterSupport_ProcessNormalMsg_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ProcessNormalMsg'
type ConsenterSupport_ProcessNormalMsg_Call struct {
	*mock.Call
}

// ProcessNormalMsg is a helper method to define mock.On call
//   - env *common.Envelope
func (_e *ConsenterSupport_Expecter) ProcessNormalMsg(env interface{}) *ConsenterSupport_ProcessNormalMsg_Call {
	return &ConsenterSupport_ProcessNormalMsg_Call{Call: _e.mock.On("ProcessNormalMsg", env)}
}

func (_c *ConsenterSupport_ProcessNormalMsg_Call) Run(run func(env *common.Envelope)) *ConsenterSupport_ProcessNormalMsg_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Envelope))
	})
	return _c
}

func (_c *ConsenterSupport_ProcessNormalMsg_Call) Return(configSeq uint64, err error) *ConsenterSupport_ProcessNormalMsg_Call {
	_c.Call.Return(configSeq, err)
	return _c
}

func (_c *ConsenterSupport_ProcessNormalMsg_Call) RunAndReturn(run func(*common.Envelope) (uint64, error)) *ConsenterSupport_ProcessNormalMsg_Call {
	_c.Call.Return(run)
	return _c
}

// Sequence provides a mock function with given fields:
func (_m *ConsenterSupport) Sequence() uint64 {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Sequence")
	}

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// ConsenterSupport_Sequence_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Sequence'
type ConsenterSupport_Sequence_Call struct {
	*mock.Call
}

// Sequence is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) Sequence() *ConsenterSupport_Sequence_Call {
	return &ConsenterSupport_Sequence_Call{Call: _e.mock.On("Sequence")}
}

func (_c *ConsenterSupport_Sequence_Call) Run(run func()) *ConsenterSupport_Sequence_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_Sequence_Call) Return(_a0 uint64) *ConsenterSupport_Sequence_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_Sequence_Call) RunAndReturn(run func() uint64) *ConsenterSupport_Sequence_Call {
	_c.Call.Return(run)
	return _c
}

// Serialize provides a mock function with given fields:
func (_m *ConsenterSupport) Serialize() ([]byte, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Serialize")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]byte, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ConsenterSupport_Serialize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Serialize'
type ConsenterSupport_Serialize_Call struct {
	*mock.Call
}

// Serialize is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) Serialize() *ConsenterSupport_Serialize_Call {
	return &ConsenterSupport_Serialize_Call{Call: _e.mock.On("Serialize")}
}

func (_c *ConsenterSupport_Serialize_Call) Run(run func()) *ConsenterSupport_Serialize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_Serialize_Call) Return(_a0 []byte, _a1 error) *ConsenterSupport_Serialize_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ConsenterSupport_Serialize_Call) RunAndReturn(run func() ([]byte, error)) *ConsenterSupport_Serialize_Call {
	_c.Call.Return(run)
	return _c
}

// SharedConfig provides a mock function with given fields:
func (_m *ConsenterSupport) SharedConfig() channelconfig.Orderer {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SharedConfig")
	}

	var r0 channelconfig.Orderer
	if rf, ok := ret.Get(0).(func() channelconfig.Orderer); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(channelconfig.Orderer)
		}
	}

	return r0
}

// ConsenterSupport_SharedConfig_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SharedConfig'
type ConsenterSupport_SharedConfig_Call struct {
	*mock.Call
}

// SharedConfig is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) SharedConfig() *ConsenterSupport_SharedConfig_Call {
	return &ConsenterSupport_SharedConfig_Call{Call: _e.mock.On("SharedConfig")}
}

func (_c *ConsenterSupport_SharedConfig_Call) Run(run func()) *ConsenterSupport_SharedConfig_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_SharedConfig_Call) Return(_a0 channelconfig.Orderer) *ConsenterSupport_SharedConfig_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_SharedConfig_Call) RunAndReturn(run func() channelconfig.Orderer) *ConsenterSupport_SharedConfig_Call {
	_c.Call.Return(run)
	return _c
}

// Sign provides a mock function with given fields: message
func (_m *ConsenterSupport) Sign(message []byte) ([]byte, error) {
	ret := _m.Called(message)

	if len(ret) == 0 {
		panic("no return value specified for Sign")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) ([]byte, error)); ok {
		return rf(message)
	}
	if rf, ok := ret.Get(0).(func([]byte) []byte); ok {
		r0 = rf(message)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(message)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ConsenterSupport_Sign_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Sign'
type ConsenterSupport_Sign_Call struct {
	*mock.Call
}

// Sign is a helper method to define mock.On call
//   - message []byte
func (_e *ConsenterSupport_Expecter) Sign(message interface{}) *ConsenterSupport_Sign_Call {
	return &ConsenterSupport_Sign_Call{Call: _e.mock.On("Sign", message)}
}

func (_c *ConsenterSupport_Sign_Call) Run(run func(message []byte)) *ConsenterSupport_Sign_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]byte))
	})
	return _c
}

func (_c *ConsenterSupport_Sign_Call) Return(_a0 []byte, _a1 error) *ConsenterSupport_Sign_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ConsenterSupport_Sign_Call) RunAndReturn(run func([]byte) ([]byte, error)) *ConsenterSupport_Sign_Call {
	_c.Call.Return(run)
	return _c
}

// SignatureVerifier provides a mock function with given fields:
func (_m *ConsenterSupport) SignatureVerifier() protoutil.BlockVerifierFunc {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for SignatureVerifier")
	}

	var r0 protoutil.BlockVerifierFunc
	if rf, ok := ret.Get(0).(func() protoutil.BlockVerifierFunc); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(protoutil.BlockVerifierFunc)
		}
	}

	return r0
}

// ConsenterSupport_SignatureVerifier_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SignatureVerifier'
type ConsenterSupport_SignatureVerifier_Call struct {
	*mock.Call
}

// SignatureVerifier is a helper method to define mock.On call
func (_e *ConsenterSupport_Expecter) SignatureVerifier() *ConsenterSupport_SignatureVerifier_Call {
	return &ConsenterSupport_SignatureVerifier_Call{Call: _e.mock.On("SignatureVerifier")}
}

func (_c *ConsenterSupport_SignatureVerifier_Call) Run(run func()) *ConsenterSupport_SignatureVerifier_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ConsenterSupport_SignatureVerifier_Call) Return(_a0 protoutil.BlockVerifierFunc) *ConsenterSupport_SignatureVerifier_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ConsenterSupport_SignatureVerifier_Call) RunAndReturn(run func() protoutil.BlockVerifierFunc) *ConsenterSupport_SignatureVerifier_Call {
	_c.Call.Return(run)
	return _c
}

// WriteBlock provides a mock function with given fields: block, encodedMetadataValue
func (_m *ConsenterSupport) WriteBlock(block *common.Block, encodedMetadataValue []byte) {
	_m.Called(block, encodedMetadataValue)
}

// ConsenterSupport_WriteBlock_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'WriteBlock'
type ConsenterSupport_WriteBlock_Call struct {
	*mock.Call
}

// WriteBlock is a helper method to define mock.On call
//   - block *common.Block
//   - encodedMetadataValue []byte
func (_e *ConsenterSupport_Expecter) WriteBlock(block interface{}, encodedMetadataValue interface{}) *ConsenterSupport_WriteBlock_Call {
	return &ConsenterSupport_WriteBlock_Call{Call: _e.mock.On("WriteBlock", block, encodedMetadataValue)}
}

func (_c *ConsenterSupport_WriteBlock_Call) Run(run func(block *common.Block, encodedMetadataValue []byte)) *ConsenterSupport_WriteBlock_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Block), args[1].([]byte))
	})
	return _c
}

func (_c *ConsenterSupport_WriteBlock_Call) Return() *ConsenterSupport_WriteBlock_Call {
	_c.Call.Return()
	return _c
}

func (_c *ConsenterSupport_WriteBlock_Call) RunAndReturn(run func(*common.Block, []byte)) *ConsenterSupport_WriteBlock_Call {
	_c.Call.Return(run)
	return _c
}

// WriteConfigBlock provides a mock function with given fields: block, encodedMetadataValue
func (_m *ConsenterSupport) WriteConfigBlock(block *common.Block, encodedMetadataValue []byte) {
	_m.Called(block, encodedMetadataValue)
}

// ConsenterSupport_WriteConfigBlock_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'WriteConfigBlock'
type ConsenterSupport_WriteConfigBlock_Call struct {
	*mock.Call
}

// WriteConfigBlock is a helper method to define mock.On call
//   - block *common.Block
//   - encodedMetadataValue []byte
func (_e *ConsenterSupport_Expecter) WriteConfigBlock(block interface{}, encodedMetadataValue interface{}) *ConsenterSupport_WriteConfigBlock_Call {
	return &ConsenterSupport_WriteConfigBlock_Call{Call: _e.mock.On("WriteConfigBlock", block, encodedMetadataValue)}
}

func (_c *ConsenterSupport_WriteConfigBlock_Call) Run(run func(block *common.Block, encodedMetadataValue []byte)) *ConsenterSupport_WriteConfigBlock_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*common.Block), args[1].([]byte))
	})
	return _c
}

func (_c *ConsenterSupport_WriteConfigBlock_Call) Return() *ConsenterSupport_WriteConfigBlock_Call {
	_c.Call.Return()
	return _c
}

func (_c *ConsenterSupport_WriteConfigBlock_Call) RunAndReturn(run func(*common.Block, []byte)) *ConsenterSupport_WriteConfigBlock_Call {
	_c.Call.Return(run)
	return _c
}

// NewConsenterSupport creates a new instance of ConsenterSupport. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConsenterSupport(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConsenterSupport {
	mock := &ConsenterSupport{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
