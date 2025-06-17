package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/colonyos/colonies/pkg/crdt"
	"github.com/openziti/sdk-golang/ziti"
)

// SyncManager manages CRDT synchronization over OpenZiti
type SyncManager struct {
	tree          *crdt.TreeCRDT
	transport     *ZitiTransport
	options       *SyncOptions
	peers         map[string]*PeerState
	peersMutex    sync.RWMutex
	syncTicker    *time.Ticker
	ctx           context.Context
	cancel        context.CancelFunc
	deltaLog      *DeltaLog
	subscriptions map[string][]string // peerID -> paths
	subMutex      sync.RWMutex
}

// PeerState tracks synchronization state with a peer
type PeerState struct {
	PeerID         string
	LastSync       time.Time
	VectorClock    *crdt.VectorClock
	Capabilities   []string
	SyncInProgress bool
	mu             sync.Mutex
}

// DeltaLog tracks changes for delta synchronization
type DeltaLog struct {
	deltas  []DeltaEntry
	mu      sync.Mutex
	maxSize int
}

// DeltaEntry represents a logged change
type DeltaEntry struct {
	Delta     crdt.Delta
	Timestamp time.Time
	Clock     *crdt.VectorClock
}

// NewSyncManager creates a new sync manager
func NewSyncManager(tree *crdt.TreeCRDT, zitiCtx ziti.Context, opts *SyncOptions) (*SyncManager, error) {
	if opts == nil {
		return nil, fmt.Errorf("sync options required")
	}

	transport, err := NewZitiTransport(zitiCtx, "crdt-sync-"+opts.TreeID, opts.NodeID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sm := &SyncManager{
		tree:          tree,
		transport:     transport,
		options:       opts,
		peers:         make(map[string]*PeerState),
		subscriptions: make(map[string][]string),
		deltaLog:      &DeltaLog{maxSize: 10000},
		ctx:           ctx,
		cancel:        cancel,
	}

	// Register message handlers
	sm.registerHandlers()

	// Subscribe to tree changes for delta logging
	if opts.EnableDeltaSync {
		sm.subscribeToChanges()
	}

	return sm, nil
}

// Start begins listening and synchronization
func (sm *SyncManager) Start() error {
	// Start listening for connections
	if err := sm.transport.Listen(sm.ctx); err != nil {
		return fmt.Errorf("failed to start listening: %w", err)
	}

	// Start periodic sync
	if sm.options.SyncInterval > 0 {
		sm.syncTicker = time.NewTicker(sm.options.SyncInterval)
		go sm.syncLoop()
	}

	return nil
}

// Stop shuts down the sync manager
func (sm *SyncManager) Stop() error {
	sm.cancel()

	if sm.syncTicker != nil {
		sm.syncTicker.Stop()
	}

	return sm.transport.Close()
}

// ConnectPeer connects to a peer for synchronization
func (sm *SyncManager) ConnectPeer(peerID, peerService string) error {
	if err := sm.transport.Connect(peerID, peerService); err != nil {
		return err
	}

	// Initialize peer state
	sm.peersMutex.Lock()
	sm.peers[peerID] = &PeerState{
		PeerID:      peerID,
		LastSync:    time.Time{},
		VectorClock: crdt.NewVectorClock(),
	}
	sm.peersMutex.Unlock()

	// Initiate sync
	return sm.syncWithPeer(peerID)
}

// registerHandlers sets up message handlers
func (sm *SyncManager) registerHandlers() {
	sm.transport.RegisterHandler(MessageTypeHello, sm.handleHello)
	sm.transport.RegisterHandler(MessageTypeSyncRequest, sm.handleSyncRequest)
	sm.transport.RegisterHandler(MessageTypeSyncResponse, sm.handleSyncResponse)
	sm.transport.RegisterHandler(MessageTypeFullState, sm.handleFullState)
	sm.transport.RegisterHandler(MessageTypeDelta, sm.handleDelta)
	sm.transport.RegisterHandler(MessageTypeVectorClock, sm.handleVectorClock)
	sm.transport.RegisterHandler(MessageTypeSubscribe, sm.handleSubscribe)
	sm.transport.RegisterHandler(MessageTypeNotification, sm.handleNotification)
}

// syncLoop performs periodic synchronization
func (sm *SyncManager) syncLoop() {
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-sm.syncTicker.C:
			sm.syncAllPeers()
		}
	}
}

// syncAllPeers syncs with all connected peers
func (sm *SyncManager) syncAllPeers() {
	sm.peersMutex.RLock()
	peers := make([]*PeerState, 0, len(sm.peers))
	for _, peer := range sm.peers {
		peers = append(peers, peer)
	}
	sm.peersMutex.RUnlock()

	for _, peer := range peers {
		go sm.syncWithPeer(peer.PeerID)
	}
}

// syncWithPeer initiates sync with a specific peer
func (sm *SyncManager) syncWithPeer(peerID string) error {
	sm.peersMutex.RLock()
	peer, exists := sm.peers[peerID]
	sm.peersMutex.RUnlock()

	if !exists {
		return fmt.Errorf("unknown peer: %s", peerID)
	}

	peer.mu.Lock()
	if peer.SyncInProgress {
		peer.mu.Unlock()
		return nil // Already syncing
	}
	peer.SyncInProgress = true
	peer.mu.Unlock()

	defer func() {
		peer.mu.Lock()
		peer.SyncInProgress = false
		peer.LastSync = time.Now()
		peer.mu.Unlock()
	}()

	// Send sync request with our vector clock
	req := &SyncMessage{
		Type:      MessageTypeSyncRequest,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	reqPayload := &SyncRequestPayload{
		VectorClock: sm.tree.GetVectorClock(),
		TreeID:      sm.options.TreeID,
	}

	req.Payload, _ = json.Marshal(reqPayload)

	return sm.transport.Send(peerID, req)
}

// handleHello processes hello messages
func (sm *SyncManager) handleHello(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload HelloPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Update peer state
	sm.peersMutex.Lock()
	sm.peers[payload.NodeID] = &PeerState{
		PeerID:       payload.NodeID,
		VectorClock:  payload.VectorClock,
		Capabilities: payload.Capabilities,
	}
	sm.peersMutex.Unlock()

	// Send hello response
	resp := &SyncMessage{
		Type:      MessageTypeHello,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	respPayload := &HelloPayload{
		Version:      "1.0",
		NodeID:       sm.options.NodeID,
		VectorClock:  sm.tree.GetVectorClock(),
		Capabilities: []string{"delta_sync", "merkle_sync", "subscriptions"},
		TreeID:       sm.options.TreeID,
	}

	resp.Payload, _ = json.Marshal(respPayload)

	return resp, nil
}

// handleSyncRequest processes sync requests
func (sm *SyncManager) handleSyncRequest(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload SyncRequestPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Check if we need to send updates
	myVC := sm.tree.GetVectorClock()
	if !sm.needsSync(payload.VectorClock, myVC) {
		// Clocks are equal, no sync needed
		return sm.createSyncResponse(false), nil
	}

	// Determine sync strategy
	if sm.options.EnableDeltaSync && sm.canUseDeltaSync(payload.VectorClock) {
		// Send deltas
		return sm.sendDeltas(msg.NodeID, payload.VectorClock)
	}

	// Fall back to full state transfer
	return sm.sendFullState(msg.NodeID)
}

// handleFullState processes full state transfers
func (sm *SyncManager) handleFullState(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload FullStatePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Deserialize and merge the tree
	otherTree := crdt.NewTreeCRDT(sm.tree.GetClientID())
	if err := otherTree.Load(payload.TreeData); err != nil {
		return nil, fmt.Errorf("failed to load tree data: %w", err)
	}

	// Merge with our tree
	if err := sm.tree.Merge(otherTree); err != nil {
		return nil, fmt.Errorf("merge failed: %w", err)
	}

	// Update peer's vector clock
	sm.peersMutex.Lock()
	if peer, exists := sm.peers[msg.NodeID]; exists {
		peer.VectorClock = otherTree.GetVectorClock()
	}
	sm.peersMutex.Unlock()

	// Send acknowledgment
	ack := &SyncMessage{
		Type:      MessageTypeAck,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	return ack, nil
}

// handleDelta processes delta updates
func (sm *SyncManager) handleDelta(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload DeltaPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Apply deltas to our tree
	for _, delta := range payload.Deltas {
		if err := sm.applyDelta(&delta); err != nil {
			return nil, fmt.Errorf("failed to apply delta: %w", err)
		}
	}

	// Update peer's vector clock
	sm.peersMutex.Lock()
	if peer, exists := sm.peers[msg.NodeID]; exists {
		peer.VectorClock = sm.tree.GetVectorClock()
	}
	sm.peersMutex.Unlock()

	// Send acknowledgment
	ack := &SyncMessage{
		Type:      MessageTypeAck,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	return ack, nil
}

// handleVectorClock processes vector clock exchanges
func (sm *SyncManager) handleVectorClock(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var vc crdt.VectorClock
	if err := json.Unmarshal(msg.Payload, &vc); err != nil {
		return nil, err
	}

	// Update peer's vector clock
	sm.peersMutex.Lock()
	if peer, exists := sm.peers[msg.NodeID]; exists {
		peer.VectorClock = &vc
	}
	sm.peersMutex.Unlock()

	// Check if sync is needed
	if sm.needsSync(&vc, sm.tree.GetVectorClock()) {
		go sm.syncWithPeer(msg.NodeID)
	}

	return nil, nil
}

// handleSubscribe processes subscription requests
func (sm *SyncManager) handleSubscribe(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload SubscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Store subscription
	sm.subMutex.Lock()
	sm.subscriptions[msg.NodeID] = payload.Paths
	sm.subMutex.Unlock()

	// Send acknowledgment
	ack := &SyncMessage{
		Type:      MessageTypeAck,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	return ack, nil
}

// handleNotification processes change notifications
func (sm *SyncManager) handleNotification(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	var payload NotificationPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, err
	}

	// Trigger sync with the peer who sent the notification
	go sm.syncWithPeer(msg.NodeID)

	return nil, nil
}

// handleSyncResponse processes sync responses
func (sm *SyncManager) handleSyncResponse(msg *SyncMessage, conn *ZitiConnection) (*SyncMessage, error) {
	// This would handle responses to our sync requests
	// Implementation depends on the specific sync strategy
	return nil, nil
}

// Helper methods

func (sm *SyncManager) needsSync(vc1, vc2 *crdt.VectorClock) bool {
	return !vc1.Equal(vc2)
}

func (sm *SyncManager) canUseDeltaSync(peerClock *crdt.VectorClock) bool {
	// Check if we have deltas from the peer's clock state
	return sm.deltaLog.hasdeltasFrom(peerClock)
}

func (sm *SyncManager) sendFullState(peerID string) (*SyncMessage, error) {
	treeData, err := sm.tree.Save()
	if err != nil {
		return nil, err
	}

	msg := &SyncMessage{
		Type:      MessageTypeFullState,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	payload := &FullStatePayload{
		TreeID:   sm.options.TreeID,
		TreeData: treeData,
	}

	msg.Payload, _ = json.Marshal(payload)

	if err := sm.transport.Send(peerID, msg); err != nil {
		return nil, err
	}

	return sm.createSyncResponse(false), nil
}

func (sm *SyncManager) sendDeltas(peerID string, since *crdt.VectorClock) (*SyncMessage, error) {
	deltas := sm.deltaLog.getDeltasSince(since)

	msg := &SyncMessage{
		Type:      MessageTypeDelta,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	payload := &DeltaPayload{
		TreeID: sm.options.TreeID,
		Deltas: deltas,
		Since:  since,
	}

	msg.Payload, _ = json.Marshal(payload)

	if err := sm.transport.Send(peerID, msg); err != nil {
		return nil, err
	}

	return sm.createSyncResponse(false), nil
}

func (sm *SyncManager) createSyncResponse(hasMore bool) *SyncMessage {
	resp := &SyncMessage{
		Type:      MessageTypeSyncResponse,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	respPayload := &SyncResponsePayload{
		TreeID:      sm.options.TreeID,
		VectorClock: sm.tree.GetVectorClock(),
		HasMore:     hasMore,
	}

	resp.Payload, _ = json.Marshal(respPayload)

	return resp
}

func (sm *SyncManager) applyDelta(delta *crdt.Delta) error {
	// This would apply a delta to the tree
	// Implementation depends on how deltas are tracked in the CRDT
	return fmt.Errorf("delta application not yet implemented")
}

func (sm *SyncManager) subscribeToChanges() {
	// Subscribe to all changes in the tree for delta logging
	sm.tree.SubscribeWithCallback("", func(event crdt.Event) {
		sm.deltaLog.addDelta(crdt.Delta{
			Operation: string(event.Type),
			Path:      event.Path,
			Clock:     sm.tree.GetVectorClock().Copy(),
		})

		// Notify subscribed peers
		sm.notifySubscribers(event)
	})
}

func (sm *SyncManager) notifySubscribers(event crdt.Event) {
	sm.subMutex.RLock()
	defer sm.subMutex.RUnlock()

	notification := &SyncMessage{
		Type:      MessageTypeNotification,
		ID:        generateMessageID(),
		NodeID:    sm.options.NodeID,
		Timestamp: time.Now(),
	}

	notifPayload := &NotificationPayload{
		TreeID:    sm.options.TreeID,
		EventType: string(event.Type),
		Path:      event.Path,
	}

	notification.Payload, _ = json.Marshal(notifPayload)

	for peerID, paths := range sm.subscriptions {
		// Check if peer is interested in this path
		if len(paths) == 0 || pathMatches(event.Path, paths) {
			sm.transport.Send(peerID, notification)
		}
	}
}

func pathMatches(eventPath string, subscribedPaths []string) bool {
	for _, path := range subscribedPaths {
		if eventPath == path || hasPrefix(eventPath, path) {
			return true
		}
	}
	return false
}

func hasPrefix(path, prefix string) bool {
	// Simple prefix check - could be enhanced with wildcard support
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

// DeltaLog methods

func (dl *DeltaLog) addDelta(delta crdt.Delta) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	dl.deltas = append(dl.deltas, DeltaEntry{
		Delta:     delta,
		Timestamp: time.Now(),
		Clock:     delta.Clock,
	})

	// Trim if too large
	if len(dl.deltas) > dl.maxSize {
		dl.deltas = dl.deltas[len(dl.deltas)-dl.maxSize:]
	}
}

func (dl *DeltaLog) getDeltasSince(since *crdt.VectorClock) []crdt.Delta {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	var result []crdt.Delta
	for _, entry := range dl.deltas {
		if entry.Clock.After(since) {
			result = append(result, entry.Delta)
		}
	}
	return result
}

func (dl *DeltaLog) hasdeltasFrom(since *crdt.VectorClock) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	for _, entry := range dl.deltas {
		if entry.Clock.Equal(since) || entry.Clock.Before(since) {
			return true
		}
	}
	return false
}
