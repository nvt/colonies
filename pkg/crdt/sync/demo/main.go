package main

import (
	"fmt"
	"github.com/colonyos/colonies/pkg/crdt/sync"
)

func main() {
	fmt.Println("Starting CRDT JSON Synchronization Demo...")
	sync.RunDemo()
}