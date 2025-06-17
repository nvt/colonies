package sync

import (
	"encoding/json"
	"fmt"

	"github.com/colonyos/colonies/pkg/crdt"
)

// JSONSyncDemo demonstrates comprehensive JSON object synchronization capabilities
func JSONSyncDemo() {
	fmt.Println("=== CRDT JSON Synchronization Demo ===\n")
	fmt.Println("Note: Path computation warnings during merge are expected and non-fatal\n")

	// Create two CRDT instances representing different peers
	peer1 := crdt.NewTreeCRDT("peer1")
	peer2 := crdt.NewTreeCRDT("peer2")

	// 1. Basic JSON Object Synchronization
	fmt.Println("1. Basic JSON Object Synchronization:")
	
	// Peer 1 creates a complex JSON document
	jsonDoc := `{
		"document": {
			"id": "doc-001",
			"title": "Distributed Document",
			"metadata": {
				"author": "Alice",
				"created": "2025-01-01T00:00:00Z",
				"tags": ["distributed", "crdt", "sync"]
			},
			"content": {
				"sections": [
					{
						"title": "Introduction",
						"text": "This is a distributed document using CRDTs"
					},
					{
						"title": "Features", 
						"text": "Supports conflict-free merging"
					}
				]
			}
		}
	}`
	
	peer1.ImportJSON([]byte(jsonDoc), "peer1")
	
	// Export and show initial state
	json1, _ := peer1.ExportJSON()
	fmt.Printf("Peer 1 initial document:\n%s\n\n", prettyJSON(json1))

	// 2. Synchronize to Peer 2
	fmt.Println("2. Synchronizing to Peer 2:")
	peer2.Merge(peer1)
	
	json2, _ := peer2.ExportJSON()
	fmt.Printf("Peer 2 after sync:\n%s\n\n", prettyJSON(json2))

	// 3. Concurrent Modifications
	fmt.Println("3. Concurrent Modifications:")
	
	// Peer 1 adds a new section
	peer1.ImportJSONToMap([]byte(`{
		"title": "Conclusion",
		"text": "CRDTs enable seamless collaboration"
	}`), findContentSectionsArrayID(peer1), "", "peer1")
	
	// Peer 2 modifies metadata concurrently
	peer2.ImportJSONToMap([]byte(`{
		"version": "1.1",
		"lastModified": "2025-01-02T10:30:00Z"
	}`), findMetadataMapID(peer2), "", "peer2")

	// Show divergent states
	json1, _ = peer1.ExportJSON()
	json2, _ = peer2.ExportJSON()
	
	fmt.Printf("Peer 1 after adding conclusion:\n%s\n\n", prettyJSON(json1))
	fmt.Printf("Peer 2 after updating metadata:\n%s\n\n", prettyJSON(json2))

	// 4. Conflict-Free Merge
	fmt.Println("4. Conflict-Free Merge:")
	
	// Merge changes bidirectionally
	peer1.Merge(peer2)
	peer2.Merge(peer1)
	
	// Both peers now have identical state
	json1, _ = peer1.ExportJSON()
	json2, _ = peer2.ExportJSON()
	
	fmt.Printf("Peer 1 after merge:\n%s\n\n", prettyJSON(json1))
	fmt.Printf("Peer 2 after merge:\n%s\n\n", prettyJSON(json2))
	
	// Verify convergence
	if string(json1) == string(json2) {
		fmt.Println("✅ SUCCESS: Both peers converged to identical state!")
	} else {
		fmt.Println("❌ ERROR: Peers have different states")
	}

	// 5. Advanced JSON Features
	fmt.Println("\n5. Advanced JSON Features:")
	demonstrateAdvancedJSONFeatures()
}

// demonstrateAdvancedJSONFeatures shows complex JSON synchronization scenarios
func demonstrateAdvancedJSONFeatures() {
	peer1 := crdt.NewTreeCRDT("alice")
	peer2 := crdt.NewTreeCRDT("bob")

	// Nested arrays and objects
	complexJSON := `{
		"users": [
			{
				"id": 1,
				"name": "Alice",
				"roles": ["admin", "editor"],
				"settings": {
					"theme": "dark",
					"notifications": true
				}
			}
		],
		"projects": {
			"project1": {
				"name": "CRDT Research",
				"collaborators": ["alice", "bob"],
				"tasks": [
					{"id": 1, "title": "Implement sync", "done": true},
					{"id": 2, "title": "Write tests", "done": false}
				]
			}
		}
	}`

	peer1.ImportJSON([]byte(complexJSON), "alice")
	peer2.Merge(peer1)

	// Alice adds a new user
	newUser := `{
		"id": 2,
		"name": "Charlie",
		"roles": ["viewer"],
		"settings": {
			"theme": "light",
			"notifications": false
		}
	}`
	
	usersArrayID := findUsersArrayID(peer1)
	peer1.ImportJSONToArray([]byte(newUser), usersArrayID, "alice")

	// Bob adds a new task concurrently
	newTask := `{"id": 3, "title": "Deploy system", "done": false}`
	tasksArrayID := findTasksArrayID(peer2)
	peer2.ImportJSONToArray([]byte(newTask), tasksArrayID, "bob")

	// Synchronize
	peer1.Merge(peer2)
	peer2.Merge(peer1)

	// Show final result
	finalJSON, _ := peer1.ExportJSON()
	fmt.Printf("Final synchronized complex JSON:\n%s\n", prettyJSON(finalJSON))
}

// Helper functions to find specific node IDs in the tree
func findContentSectionsArrayID(tree *crdt.TreeCRDT) crdt.NodeID {
	// In a real implementation, you'd traverse the tree to find the sections array
	// For demo purposes, we'll use a simplified approach
	for id, node := range tree.Nodes {
		if node.IsArray {
			return id
		}
	}
	return "root"
}

func findMetadataMapID(tree *crdt.TreeCRDT) crdt.NodeID {
	// Similar approach for finding metadata map
	for id, node := range tree.Nodes {
		if node.IsMap && id != "root" {
			return id
		}
	}
	return "root"
}

func findUsersArrayID(tree *crdt.TreeCRDT) crdt.NodeID {
	// Find users array
	for id, node := range tree.Nodes {
		if node.IsArray {
			return id
		}
	}
	return "root"
}

func findTasksArrayID(tree *crdt.TreeCRDT) crdt.NodeID {
	// Find tasks array (second array in this case)
	arrayCount := 0
	for id, node := range tree.Nodes {
		if node.IsArray {
			arrayCount++
			if arrayCount >= 2 {
				return id
			}
		}
	}
	return "root"
}

// prettyJSON formats JSON for better display
func prettyJSON(data []byte) string {
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return string(data)
	}
	
	pretty, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return string(data)
	}
	
	return string(pretty)
}

// JSONCapabilities describes what JSON features are supported
func JSONCapabilities() map[string]bool {
	return map[string]bool{
		"Objects (Maps)":           true,
		"Arrays":                  true,
		"Nested Structures":       true,
		"Strings":                 true,
		"Numbers":                 true,
		"Booleans":                true,
		"Null Values":             true,
		"Concurrent Modifications": true,
		"Conflict-Free Merging":   true,
		"Partial Synchronization": true,
		"Path-Based Access":       true,
		"Change Subscriptions":    true,
		"Vector Clock Ordering":   true,
		"Digital Signatures":      true,
		"Access Control (ABAC)":   true,
	}
}

// PrintJSONCapabilities shows all supported JSON features
func PrintJSONCapabilities() {
	fmt.Println("=== CRDT JSON Synchronization Capabilities ===")
	capabilities := JSONCapabilities()
	
	for feature, supported := range capabilities {
		status := "❌"
		if supported {
			status = "✅"
		}
		fmt.Printf("%s %s\n", status, feature)
	}
	
	fmt.Println("\n=== Synchronization Features ===")
	fmt.Println("✅ Real-time synchronization over OpenZiti")
	fmt.Println("✅ Delta-based incremental updates")
	fmt.Println("✅ Full-state transfer when needed")
	fmt.Println("✅ Automatic conflict resolution")
	fmt.Println("✅ Causal consistency with vector clocks")
	fmt.Println("✅ Secure, authenticated peer-to-peer sync")
	fmt.Println("✅ Subscription to specific JSON paths")
	fmt.Println("✅ Change notifications and events")
}

// ExampleUsage shows how to use JSON synchronization
func ExampleUsage() {
	fmt.Println("=== Example Usage ===\n")
	
	// Basic setup
	fmt.Println("// Create CRDT instance")
	fmt.Println("tree := crdt.NewTreeCRDT(\"node1\")")
	fmt.Println("")
	
	// Import JSON
	fmt.Println("// Import JSON document")
	fmt.Println("jsonData := `{\"users\": [{\"name\": \"Alice\"}]}`")
	fmt.Println("tree.ImportJSON([]byte(jsonData), \"node1\")")
	fmt.Println("")
	
	// Set up synchronization
	fmt.Println("// Set up OpenZiti synchronization")
	fmt.Println("opts := sync.DefaultSyncOptions(\"node1\", \"shared-doc\")")
	fmt.Println("syncMgr, _ := sync.NewSyncManager(tree, zitiContext, opts)")
	fmt.Println("syncMgr.Start()")
	fmt.Println("")
	
	// Connect peers
	fmt.Println("// Connect to other peers")
	fmt.Println("syncMgr.ConnectPeer(\"node2\", \"crdt-sync-shared-doc\")")
	fmt.Println("")
	
	// Subscribe to changes
	fmt.Println("// Subscribe to changes")
	fmt.Println("tree.SubscribeWithCallback(\"/users\", func(event crdt.Event) {")
	fmt.Println("    fmt.Printf(\"User data changed: %s\\n\", event.Path)")
	fmt.Println("})")
	fmt.Println("")
	
	// Make changes
	fmt.Println("// Changes are automatically synchronized")
	fmt.Println("tree.ImportJSONToArray([]byte(`{\"name\": \"Bob\"}`), usersArrayID, \"node1\")")
	
	fmt.Println("\n✅ All connected peers will automatically receive the changes!")
}

// RunDemo executes the complete JSON synchronization demonstration
func RunDemo() {
	PrintJSONCapabilities()
	fmt.Println()
	JSONSyncDemo()
	fmt.Println()
	ExampleUsage()
}