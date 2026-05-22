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
	db, err := engine.Open("./data")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tcpServer := server.NewServer(":8080", db)

	go func() {
		if err := tcpServer.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	<-stopChan

	log.Println("\nShutdown signal received...")
	tcpServer.Stop()
}
