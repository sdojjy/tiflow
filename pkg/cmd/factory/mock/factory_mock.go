// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/cmd/factory/factory.go

// Package mock_factory is a generated GoMock package.
package mock_factory

import (
	tls "crypto/tls"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "github.com/pingcap/tiflow/pkg/api/v1"
	v2 "github.com/pingcap/tiflow/pkg/api/v2"
	etcd "github.com/pingcap/tiflow/pkg/etcd"
	security "github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	grpc "google.golang.org/grpc"
)

// MockFactory is a mock of Factory interface.
type MockFactory struct {
	ctrl     *gomock.Controller
	recorder *MockFactoryMockRecorder
}

// MockFactoryMockRecorder is the mock recorder for MockFactory.
type MockFactoryMockRecorder struct {
	mock *MockFactory
}

// NewMockFactory creates a new mock instance.
func NewMockFactory(ctrl *gomock.Controller) *MockFactory {
	mock := &MockFactory{ctrl: ctrl}
	mock.recorder = &MockFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFactory) EXPECT() *MockFactoryMockRecorder {
	return m.recorder
}

// APIV1Client mocks base method.
func (m *MockFactory) APIV1Client() (v1.APIV1Interface, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "APIV1Client")
	ret0, _ := ret[0].(v1.APIV1Interface)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// APIV1Client indicates an expected call of APIV1Client.
func (mr *MockFactoryMockRecorder) APIV1Client() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "APIV1Client", reflect.TypeOf((*MockFactory)(nil).APIV1Client))
}

// APIV2Client mocks base method.
func (m *MockFactory) APIV2Client() (v2.APIV2Interface, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "APIV2Client")
	ret0, _ := ret[0].(v2.APIV2Interface)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// APIV2Client indicates an expected call of APIV2Client.
func (mr *MockFactoryMockRecorder) APIV2Client() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "APIV2Client", reflect.TypeOf((*MockFactory)(nil).APIV2Client))
}

// EtcdClient mocks base method.
func (m *MockFactory) EtcdClient() (*etcd.CDCEtcdClientImpl, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EtcdClient")
	ret0, _ := ret[0].(*etcd.CDCEtcdClientImpl)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// EtcdClient indicates an expected call of EtcdClient.
func (mr *MockFactoryMockRecorder) EtcdClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EtcdClient", reflect.TypeOf((*MockFactory)(nil).EtcdClient))
}

// GetCredential mocks base method.
func (m *MockFactory) GetCredential() *security.Credential {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCredential")
	ret0, _ := ret[0].(*security.Credential)
	return ret0
}

// GetCredential indicates an expected call of GetCredential.
func (mr *MockFactoryMockRecorder) GetCredential() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCredential", reflect.TypeOf((*MockFactory)(nil).GetCredential))
}

// GetLogLevel mocks base method.
func (m *MockFactory) GetLogLevel() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLogLevel")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetLogLevel indicates an expected call of GetLogLevel.
func (mr *MockFactoryMockRecorder) GetLogLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLogLevel", reflect.TypeOf((*MockFactory)(nil).GetLogLevel))
}

// GetPdAddr mocks base method.
func (m *MockFactory) GetPdAddr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPdAddr")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetPdAddr indicates an expected call of GetPdAddr.
func (mr *MockFactoryMockRecorder) GetPdAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPdAddr", reflect.TypeOf((*MockFactory)(nil).GetPdAddr))
}

// GetServerAddr mocks base method.
func (m *MockFactory) GetServerAddr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetServerAddr")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetServerAddr indicates an expected call of GetServerAddr.
func (mr *MockFactoryMockRecorder) GetServerAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetServerAddr", reflect.TypeOf((*MockFactory)(nil).GetServerAddr))
}

// PdClient mocks base method.
func (m *MockFactory) PdClient() (pd.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PdClient")
	ret0, _ := ret[0].(pd.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PdClient indicates an expected call of PdClient.
func (mr *MockFactoryMockRecorder) PdClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PdClient", reflect.TypeOf((*MockFactory)(nil).PdClient))
}

// ToGRPCDialOption mocks base method.
func (m *MockFactory) ToGRPCDialOption() (grpc.DialOption, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToGRPCDialOption")
	ret0, _ := ret[0].(grpc.DialOption)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ToGRPCDialOption indicates an expected call of ToGRPCDialOption.
func (mr *MockFactoryMockRecorder) ToGRPCDialOption() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToGRPCDialOption", reflect.TypeOf((*MockFactory)(nil).ToGRPCDialOption))
}

// ToTLSConfig mocks base method.
func (m *MockFactory) ToTLSConfig() (*tls.Config, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToTLSConfig")
	ret0, _ := ret[0].(*tls.Config)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ToTLSConfig indicates an expected call of ToTLSConfig.
func (mr *MockFactoryMockRecorder) ToTLSConfig() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToTLSConfig", reflect.TypeOf((*MockFactory)(nil).ToTLSConfig))
}

// MockClientGetter is a mock of ClientGetter interface.
type MockClientGetter struct {
	ctrl     *gomock.Controller
	recorder *MockClientGetterMockRecorder
}

// MockClientGetterMockRecorder is the mock recorder for MockClientGetter.
type MockClientGetterMockRecorder struct {
	mock *MockClientGetter
}

// NewMockClientGetter creates a new mock instance.
func NewMockClientGetter(ctrl *gomock.Controller) *MockClientGetter {
	mock := &MockClientGetter{ctrl: ctrl}
	mock.recorder = &MockClientGetterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClientGetter) EXPECT() *MockClientGetterMockRecorder {
	return m.recorder
}

// GetCredential mocks base method.
func (m *MockClientGetter) GetCredential() *security.Credential {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCredential")
	ret0, _ := ret[0].(*security.Credential)
	return ret0
}

// GetCredential indicates an expected call of GetCredential.
func (mr *MockClientGetterMockRecorder) GetCredential() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCredential", reflect.TypeOf((*MockClientGetter)(nil).GetCredential))
}

// GetLogLevel mocks base method.
func (m *MockClientGetter) GetLogLevel() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLogLevel")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetLogLevel indicates an expected call of GetLogLevel.
func (mr *MockClientGetterMockRecorder) GetLogLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLogLevel", reflect.TypeOf((*MockClientGetter)(nil).GetLogLevel))
}

// GetPdAddr mocks base method.
func (m *MockClientGetter) GetPdAddr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPdAddr")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetPdAddr indicates an expected call of GetPdAddr.
func (mr *MockClientGetterMockRecorder) GetPdAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPdAddr", reflect.TypeOf((*MockClientGetter)(nil).GetPdAddr))
}

// GetServerAddr mocks base method.
func (m *MockClientGetter) GetServerAddr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetServerAddr")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetServerAddr indicates an expected call of GetServerAddr.
func (mr *MockClientGetterMockRecorder) GetServerAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetServerAddr", reflect.TypeOf((*MockClientGetter)(nil).GetServerAddr))
}

// ToGRPCDialOption mocks base method.
func (m *MockClientGetter) ToGRPCDialOption() (grpc.DialOption, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToGRPCDialOption")
	ret0, _ := ret[0].(grpc.DialOption)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ToGRPCDialOption indicates an expected call of ToGRPCDialOption.
func (mr *MockClientGetterMockRecorder) ToGRPCDialOption() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToGRPCDialOption", reflect.TypeOf((*MockClientGetter)(nil).ToGRPCDialOption))
}

// ToTLSConfig mocks base method.
func (m *MockClientGetter) ToTLSConfig() (*tls.Config, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToTLSConfig")
	ret0, _ := ret[0].(*tls.Config)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ToTLSConfig indicates an expected call of ToTLSConfig.
func (mr *MockClientGetterMockRecorder) ToTLSConfig() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToTLSConfig", reflect.TypeOf((*MockClientGetter)(nil).ToTLSConfig))
}