package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NonOrdinary/titankv/internal/engine"
	"github.com/NonOrdinary/titankv/internal/server"
)

func main() {
	// 1. Initialize the Storage Engine (Phases 1-3)
	// It will create a "data" directory in the root of your project
	db, err := engine.Open("./data")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// 2. Initialize the TCP Server (Phase 4)
	tcpServer := server.NewServer(":8080", db)

	// 3. Start the server in a separate goroutine
	go func() {
		if err := tcpServer.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// 4. Wait for Ctrl+C to test our Graceful Shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan // Blocks here until you press Ctrl+C

	log.Println("\nShutdown signal received...")
	tcpServer.Stop() // Stops network connections safely
}
