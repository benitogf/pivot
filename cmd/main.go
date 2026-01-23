package main

import (
	"log"

	"github.com/benitogf/ooo"
	"github.com/benitogf/pivot"
)

func main() {
	server := &ooo.Server{}

	// Configure pivot synchronization
	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "settings"}, // Single item sync
			{Path: "things/*"}, // List sync
		},
		NodesKey:        "devices/*",      // Node discovery path
		ClusterURL:      "localhost:8888", // Address of the pivot server
		AutoSyncOnStart: true,
	}

	server.OpenFilter("state")
	server.OpenFilter("logs/*")
	// Setup pivot - modifies server routes and storage hooks
	server = pivot.Setup(server, config)
	server.Start("0.0.0.0:8801")
	log.Println("Node server running on :8801, syncing with pivot at localhost:8800")
	server.WaitClose()
}
