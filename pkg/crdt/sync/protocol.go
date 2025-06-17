package sync

import (
	"encoding/json"
	"time"

	"github.com/colonyos/colonies/pkg/crdt"
)

// MessageType represents different sync protocol message types
type MessageType uint8

const (
	// Basic sync messages
	MessageTypeHello        MessageType = iota // Initial handshake
	MessageTypeSyncRequest                     // Request sync from peer
	MessageTypeSyncResponse                    // Response with sync data
	MessageTypeFullState                       // Full CRDT state transfer
	MessageTypeDelta                           // Incremental changes
	MessageTypeVectorClock                     // Vector clock exchange
	MessageTypeAck                             // Acknowledgment
	MessageTypeError                           // Error message

	// Advanced sync messages
	MessageTypeMerkleRoot     // Merkle tree root for efficient diff
	MessageTypeMerkleRequest  // Request specific subtree
	MessageTypeMerkleResponse // Response with subtree data
	MessageTypeSubscribe      // Subscribe to changes
	MessageTypeUnsubscribe    // Unsubscribe from changes
	MessageTypeNotification   // Change notification
)

// SyncMessage is the base message structure for all sync protocol messages
type SyncMessage struct {
	Type      MessageType     `json:"type"`
	ID        string          `json:"id"`      // Message ID for tracking
	NodeID    string          `json:"node_id"` // Sender node ID
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// HelloPayload is sent during initial handshake
type HelloPayload struct {
	Version      string            `json:"version"`
	NodeID       string            `json:"node_id"`
	VectorClock  *crdt.VectorClock `json:"vector_clock"`
	Capabilities []string          `json:"capabilities"` // Supported sync features
	TreeID       string            `json:"tree_id"`      // CRDT tree identifier
}

// SyncRequestPayload requests synchronization from a peer
type SyncRequestPayload struct {
	VectorClock *crdt.VectorClock `json:"vector_clock"`
	TreeID      string            `json:"tree_id"`
	// Optional: specify paths to sync only parts of the tree
	Paths []string `json:"paths,omitempty"`
}

// SyncResponsePayload contains sync data
type SyncResponsePayload struct {
	TreeID      string            `json:"tree_id"`
	VectorClock *crdt.VectorClock `json:"vector_clock"`
	HasMore     bool              `json:"has_more"` // For chunked responses
	ChunkID     int               `json:"chunk_id"`
}

// FullStatePayload contains complete CRDT state
type FullStatePayload struct {
	TreeID   string          `json:"tree_id"`
	TreeData json.RawMessage `json:"tree_data"` // Serialized TreeCRDT
}

// DeltaPayload contains incremental changes
type DeltaPayload struct {
	TreeID string            `json:"tree_id"`
	Deltas []crdt.Delta      `json:"deltas"`
	Since  *crdt.VectorClock `json:"since"` // Vector clock when deltas start
}

// MerkleRootPayload for efficient sync negotiation
type MerkleRootPayload struct {
	TreeID     string `json:"tree_id"`
	RootHash   string `json:"root_hash"`
	TreeHeight int    `json:"tree_height"`
}

// MerkleRequestPayload requests specific subtrees
type MerkleRequestPayload struct {
	TreeID string   `json:"tree_id"`
	Paths  []string `json:"paths"` // Paths to subtrees needed
}

// SubscribePayload for change notifications
type SubscribePayload struct {
	TreeID string   `json:"tree_id"`
	Paths  []string `json:"paths"` // Empty means subscribe to all
}

// NotificationPayload for change events
type NotificationPayload struct {
	TreeID    string          `json:"tree_id"`
	EventType string          `json:"event_type"` // added, updated, removed
	Path      string          `json:"path"`
	NodeData  json.RawMessage `json:"node_data,omitempty"`
}

// ErrorPayload for error responses
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// SyncProtocol defines the synchronization protocol interface
type SyncProtocol interface {
	// HandleMessage processes incoming sync messages
	HandleMessage(msg *SyncMessage) (*SyncMessage, error)

	// GetVectorClock returns current vector clock
	GetVectorClock() *crdt.VectorClock

	// NeedsSync checks if sync is needed with given vector clock
	NeedsSync(peerClock *crdt.VectorClock) bool
}

// SyncOptions configures sync behavior
type SyncOptions struct {
	// Basic options
	NodeID         string
	TreeID         string
	SyncInterval   time.Duration
	MaxMessageSize int
	EnableChunking bool
	ChunkSize      int

	// Advanced options
	EnableMerkleSync    bool
	EnableDeltaSync     bool
	EnableSubscriptions bool
	ConflictResolution  string // "lww", "custom"

	// Security options
	EnableEncryption     bool
	EnableAuthentication bool
	RequireSignatures    bool
}

// DefaultSyncOptions returns recommended default options
func DefaultSyncOptions(nodeID, treeID string) *SyncOptions {
	return &SyncOptions{
		NodeID:               nodeID,
		TreeID:               treeID,
		SyncInterval:         5 * time.Second,
		MaxMessageSize:       1024 * 1024, // 1MB
		EnableChunking:       true,
		ChunkSize:            512 * 1024, // 512KB chunks
		EnableMerkleSync:     true,
		EnableDeltaSync:      true,
		EnableSubscriptions:  true,
		ConflictResolution:   "lww",
		EnableEncryption:     true,
		EnableAuthentication: true,
		RequireSignatures:    true,
	}
}
