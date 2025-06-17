package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/openziti/sdk-golang/ziti"
)

// ZitiTransport handles OpenZiti network communication for CRDT sync
type ZitiTransport struct {
	ctx          ziti.Context
	serviceName  string
	nodeID       string
	listener     net.Listener
	connections  map[string]*ZitiConnection
	connMutex    sync.RWMutex
	handlers     map[MessageType]MessageHandler
	handlerMutex sync.RWMutex
	options      *TransportOptions
	closed       bool
	closeMutex   sync.Mutex
}

// ZitiConnection represents a connection to a peer
type ZitiConnection struct {
	conn         net.Conn
	peerID       string
	encoder      *json.Encoder
	decoder      *json.Decoder
	sendQueue    chan *SyncMessage
	closeOnce    sync.Once
	lastActivity time.Time
	mu           sync.Mutex
}

// MessageHandler processes incoming messages
type MessageHandler func(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error)

// TransportOptions configures the transport layer
type TransportOptions struct {
	MaxConnections       int
	ConnectionTimeout    time.Duration
	HeartbeatInterval    time.Duration
	MessageTimeout       time.Duration
	QueueSize            int
	EnableCompression    bool
	EnableEncryption     bool
	ReconnectInterval    time.Duration
	MaxReconnectAttempts int
}

// DefaultTransportOptions returns recommended defaults
func DefaultTransportOptions() *TransportOptions {
	return &TransportOptions{
		MaxConnections:       100,
		ConnectionTimeout:    30 * time.Second,
		HeartbeatInterval:    30 * time.Second,
		MessageTimeout:       10 * time.Second,
		QueueSize:            1000,
		EnableCompression:    true,
		EnableEncryption:     true,
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 5,
	}
}

// NewZitiTransport creates a new OpenZiti transport
func NewZitiTransport(ctx ziti.Context, serviceName, nodeID string, opts *TransportOptions) (*ZitiTransport, error) {
	if opts == nil {
		opts = DefaultTransportOptions()
	}

	return &ZitiTransport{
		ctx:         ctx,
		serviceName: serviceName,
		nodeID:      nodeID,
		connections: make(map[string]*ZitiConnection),
		handlers:    make(map[MessageType]MessageHandler),
		options:     opts,
	}, nil
}

// RegisterHandler registers a message handler for a specific message type
func (t *ZitiTransport) RegisterHandler(msgType MessageType, handler MessageHandler) {
	t.handlerMutex.Lock()
	defer t.handlerMutex.Unlock()
	t.handlers[msgType] = handler
}

// Listen starts listening for incoming connections
func (t *ZitiTransport) Listen(ctx context.Context) error {
	listener, err := t.ctx.Listen(t.serviceName)
	if err != nil {
		return fmt.Errorf("failed to listen on service %s: %w", t.serviceName, err)
	}

	t.listener = listener

	go t.acceptConnections(ctx)

	return nil
}

// Connect establishes a connection to a peer
func (t *ZitiTransport) Connect(peerID string, peerService string) error {
	t.connMutex.Lock()
	if _, exists := t.connections[peerID]; exists {
		t.connMutex.Unlock()
		return fmt.Errorf("already connected to peer %s", peerID)
	}
	t.connMutex.Unlock()

	conn, err := t.ctx.Dial(peerService)
	if err != nil {
		return fmt.Errorf("failed to dial peer %s at service %s: %w", peerID, peerService, err)
	}

	zitiConn := t.createConnection(conn, peerID)

	t.connMutex.Lock()
	t.connections[peerID] = zitiConn
	t.connMutex.Unlock()

	go t.handleConnection(zitiConn)

	// Send hello message
	hello := &SyncMessage{
		Type:      MessageTypeHello,
		ID:        generateMessageID(),
		NodeID:    t.nodeID,
		Timestamp: time.Now(),
	}

	helloPayload := &HelloPayload{
		Version:      "1.0",
		NodeID:       t.nodeID,
		Capabilities: []string{"delta_sync", "merkle_sync", "subscriptions"},
	}

	hello.Payload, _ = json.Marshal(helloPayload)

	return zitiConn.Send(hello)
}

// Send sends a message to a specific peer
func (t *ZitiTransport) Send(peerID string, msg *SyncMessage) error {
	t.connMutex.RLock()
	conn, exists := t.connections[peerID]
	t.connMutex.RUnlock()

	if !exists {
		return fmt.Errorf("not connected to peer %s", peerID)
	}

	return conn.Send(msg)
}

// Broadcast sends a message to all connected peers
func (t *ZitiTransport) Broadcast(msg *SyncMessage) error {
	t.connMutex.RLock()
	connections := make([]*ZitiConnection, 0, len(t.connections))
	for _, conn := range t.connections {
		connections = append(connections, conn)
	}
	t.connMutex.RUnlock()

	var firstErr error
	for _, conn := range connections {
		if err := conn.Send(msg); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Close shuts down the transport
func (t *ZitiTransport) Close() error {
	t.closeMutex.Lock()
	defer t.closeMutex.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	// Close listener
	if t.listener != nil {
		t.listener.Close()
	}

	// Close all connections
	t.connMutex.Lock()
	for peerID, conn := range t.connections {
		conn.Close()
		delete(t.connections, peerID)
	}
	t.connMutex.Unlock()

	return nil
}

// acceptConnections handles incoming connections
func (t *ZitiTransport) acceptConnections(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := t.listener.Accept()
			if err != nil {
				if t.closed {
					return
				}
				continue
			}

			// Create connection with temporary ID until we receive hello
			zitiConn := t.createConnection(conn, "")
			go t.handleConnection(zitiConn)
		}
	}
}

// handleConnection manages a single connection
func (t *ZitiTransport) handleConnection(conn *ZitiConnection) {
	defer conn.Close()

	// Start send loop
	go conn.sendLoop()

	// Read messages
	for {
		var msg SyncMessage
		if err := conn.decoder.Decode(&msg); err != nil {
			if err != io.EOF {
				// Log error
			}
			break
		}

		conn.updateActivity()

		// Handle hello message specially to get peer ID
		if msg.Type == MessageTypeHello && conn.peerID == "" {
			var payload HelloPayload
			if err := json.Unmarshal(msg.Payload, &payload); err == nil {
				conn.peerID = payload.NodeID
				t.connMutex.Lock()
				t.connections[conn.peerID] = conn
				t.connMutex.Unlock()
			}
		}

		// Process message
		t.handlerMutex.RLock()
		handler, exists := t.handlers[msg.Type]
		t.handlerMutex.RUnlock()

		if exists {
			response, err := handler(&msg, conn)
			if err != nil {
				// Send error response
				errResp := &SyncMessage{
					Type:      MessageTypeError,
					ID:        generateMessageID(),
					NodeID:    t.nodeID,
					Timestamp: time.Now(),
				}
				errPayload := &ErrorPayload{
					Code:    "HANDLER_ERROR",
					Message: err.Error(),
				}
				errResp.Payload, _ = json.Marshal(errPayload)
				conn.Send(errResp)
			} else if response != nil {
				conn.Send(response)
			}
		}
	}

	// Remove from connections
	if conn.peerID != "" {
		t.connMutex.Lock()
		delete(t.connections, conn.peerID)
		t.connMutex.Unlock()
	}
}

// createConnection creates a new ZitiConnection
func (t *ZitiTransport) createConnection(conn net.Conn, peerID string) *ZitiConnection {
	return &ZitiConnection{
		conn:         conn,
		peerID:       peerID,
		encoder:      json.NewEncoder(conn),
		decoder:      json.NewDecoder(conn),
		sendQueue:    make(chan *SyncMessage, t.options.QueueSize),
		lastActivity: time.Now(),
	}
}

// Send queues a message for sending
func (c *ZitiConnection) Send(msg *SyncMessage) error {
	select {
	case c.sendQueue <- msg:
		return nil
	default:
		return fmt.Errorf("send queue full")
	}
}

// sendLoop handles outgoing messages
func (c *ZitiConnection) sendLoop() {
	for msg := range c.sendQueue {
		if err := c.encoder.Encode(msg); err != nil {
			// Log error
			break
		}
	}
}

// Close closes the connection
func (c *ZitiConnection) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.sendQueue)
		err = c.conn.Close()
	})
	return err
}

// updateActivity updates last activity time
func (c *ZitiConnection) updateActivity() {
	c.mu.Lock()
	c.lastActivity = time.Now()
	c.mu.Unlock()
}

// generateMessageID generates a unique message ID
func generateMessageID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Nanosecond())
}
