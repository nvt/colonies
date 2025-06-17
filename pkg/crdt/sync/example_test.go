package sync_test

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/crdt"
	"github.com/colonyos/colonies/pkg/crdt/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockZitiContext implements a basic ziti.Context for testing
type MockZitiContext struct {
	listeners map[string]*MockListener
	dialers   map[string]*MockConnection
}

type MockListener struct {
	serviceName string
	acceptCh    chan *MockConnection
}

type MockConnection struct {
	data chan []byte
	peer string
}

func (m *MockZitiContext) Listen(serviceName string) (net.Listener, error) {
	listener := &MockListener{
		serviceName: serviceName,
		acceptCh:    make(chan *MockConnection, 10),
	}
	if m.listeners == nil {
		m.listeners = make(map[string]*MockListener)
	}
	m.listeners[serviceName] = listener
	return listener, nil
}

func (m *MockZitiContext) Dial(serviceName string) (net.Conn, error) {
	conn := &MockConnection{
		data: make(chan []byte, 100),
		peer: serviceName,
	}
	if m.dialers == nil {
		m.dialers = make(map[string]*MockConnection)
	}
	m.dialers[serviceName] = conn

	// Simulate connection to listener
	if listener, exists := m.listeners[serviceName]; exists {
		go func() {
			listener.acceptCh <- conn
		}()
	}

	return conn, nil
}

func (l *MockListener) Accept() (net.Conn, error) {
	conn := <-l.acceptCh
	return conn, nil
}

func (l *MockListener) Close() error {
	close(l.acceptCh)
	return nil
}

func (l *MockListener) Addr() net.Addr {
	return &MockAddr{address: l.serviceName}
}

type MockAddr struct {
	address string
}

func (a *MockAddr) Network() string {
	return "ziti"
}

func (a *MockAddr) String() string {
	return a.address
}

func (c *MockConnection) Read(b []byte) (int, error) {
	data := <-c.data
	copy(b, data)
	return len(data), nil
}

func (c *MockConnection) Write(b []byte) (int, error) {
	dataCopy := make([]byte, len(b))
	copy(dataCopy, b)
	c.data <- dataCopy
	return len(b), nil
}

func (c *MockConnection) Close() error {
	close(c.data)
	return nil
}

func (c *MockConnection) LocalAddr() net.Addr {
	return &MockAddr{address: "local"}
}

func (c *MockConnection) RemoteAddr() net.Addr {
	return &MockAddr{address: c.peer}
}

func (c *MockConnection) SetDeadline(t time.Time) error {
	return nil
}

func (c *MockConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *MockConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// TestCRDTSynchronizationExample demonstrates basic CRDT synchronization
func TestCRDTSynchronizationExample(t *testing.T) {
	// Create TreeCRDTs directly for this example
	tree1 := crdt.NewTreeCRDT("client1")
	tree2 := crdt.NewTreeCRDT("client2")

	// For this example, we'll skip the OpenZiti integration
	// and just demonstrate the synchronization logic

	// Make changes to tree1
	tree1.ImportJSON([]byte(`{"hello": "world"}`), "client1")

	// Simulate synchronization by merging trees
	tree2.Merge(tree1)

	// Check that tree2 has been updated
	json1, _ := tree1.ExportJSON()
	json2, _ := tree2.ExportJSON()

	// Verify synchronization worked
	assert.Equal(t, string(json1), string(json2))
	assert.Contains(t, string(json1), "hello")
	assert.Contains(t, string(json1), "world")
}

// TestCRDTSyncBasic tests basic synchronization functionality
func TestCRDTSyncBasic(t *testing.T) {
	// Note: This is a simplified test that would need actual Ziti infrastructure
	// In a real environment, you would have Ziti controllers and edge routers set up

	t.Skip("Requires actual Ziti infrastructure for integration testing")

	// This test would:
	// 1. Set up two TreeCRDTs with different initial states
	// 2. Create sync managers with real Ziti contexts
	// 3. Make concurrent changes to both trees
	// 4. Verify that both trees converge to the same state
	// 5. Test conflict resolution mechanisms
	// 6. Test subscription/notification functionality
}

// TestCRDTSyncProtocol tests the sync protocol message handling
func TestCRDTSyncProtocol(t *testing.T) {
	// Test hello message
	hello := &sync.HelloPayload{
		Version:      "1.0",
		NodeID:       "test-node",
		VectorClock:  crdt.NewVectorClock(),
		Capabilities: []string{"delta_sync", "merkle_sync"},
		TreeID:       "test-tree",
	}

	msg := &sync.SyncMessage{
		Type:      sync.MessageTypeHello,
		ID:        "msg-1",
		NodeID:    "test-node",
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(hello)
	require.NoError(t, err)
	msg.Payload = payload

	// Verify message can be serialized/deserialized
	msgBytes, err := json.Marshal(msg)
	require.NoError(t, err)

	var decodedMsg sync.SyncMessage
	err = json.Unmarshal(msgBytes, &decodedMsg)
	require.NoError(t, err)

	assert.Equal(t, sync.MessageTypeHello, decodedMsg.Type)
	assert.Equal(t, "test-node", decodedMsg.NodeID)

	var decodedHello sync.HelloPayload
	err = json.Unmarshal(decodedMsg.Payload, &decodedHello)
	require.NoError(t, err)

	assert.Equal(t, "1.0", decodedHello.Version)
	assert.Equal(t, "test-node", decodedHello.NodeID)
	assert.Contains(t, decodedHello.Capabilities, "delta_sync")
}

// TestSyncOptions tests sync option validation
func TestSyncOptions(t *testing.T) {
	opts := sync.DefaultSyncOptions("node1", "tree1")

	assert.Equal(t, "node1", opts.NodeID)
	assert.Equal(t, "tree1", opts.TreeID)
	assert.Equal(t, 5*time.Second, opts.SyncInterval)
	assert.True(t, opts.EnableMerkleSync)
	assert.True(t, opts.EnableDeltaSync)
	assert.True(t, opts.EnableSubscriptions)
	assert.Equal(t, "lww", opts.ConflictResolution)
}
