package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NonOrdinary/titankv/internal/cluster"
	"github.com/NonOrdinary/titankv/internal/engine"
	"github.com/NonOrdinary/titankv/internal/server"
)

func main() {
	fmt.Println("🚀 Booting TitanKV Distributed Cluster...")

	// 1. Clean up old test data
	os.RemoveAll("./data_shard_1")
	os.RemoveAll("./data_shard_2")
	os.RemoveAll("./data_shard_3")

	// 2. Boot 3 Physical Storage Engines (Phases 1-3)
	db1, _ := engine.Open("./data_shard_1")
	db2, _ := engine.Open("./data_shard_2")
	db3, _ := engine.Open("./data_shard_3")
	defer db1.Close()
	defer db2.Close()
	defer db3.Close()

	go server.NewServer("127.0.0.1:8081", db1).Start()
	go server.NewServer("127.0.0.1:8082", db2).Start()
	go server.NewServer("127.0.0.1:8083", db3).Start()

	// 4. Configure Consistent Hashing
	ring := cluster.NewHashRing(3) // 3 Virtual Nodes per physical node
	ring.AddNode("127.0.0.1:8081")
	ring.AddNode("127.0.0.1:8082")
	ring.AddNode("127.0.0.1:8083")

	// 5. Boot the API Gateway Router
	gateway := cluster.NewRouter("127.0.0.1:8000", ring)
	go gateway.Start()

	// Give the TCP sockets a second to bind to the OS
	time.Sleep(1 * time.Second)
	fmt.Println("✅ Cluster Online. Gateway running on :8000")
	fmt.Println("🔥 Initiating Stress Test: 100,000 Concurrent Writes...")

	// --- THE STRESS TEST ---
	numWorkers := 100
	requestsPerWorker := 1000

	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	startTime := time.Now()

	// Spawn 100 concurrent clients
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker dials the Gateway Router, NOT the individual databases
			client, err := server.NewClient("127.0.0.1:8000")
			if err != nil {
				atomic.AddInt32(&failCount, int32(requestsPerWorker))
				log.Printf("Worker %d failed to connect: %v", workerID, err)
				return
			}
			defer client.Close()

			for i := 0; i < requestsPerWorker; i++ {
				// Generate a unique key for every request
				key := fmt.Sprintf("user_%d_data_%d", workerID, i)
				value := []byte("high_performance_payload_data")

				// Send the PUT request through the router
				err := client.Put(key, value)
				if err != nil {
					atomic.AddInt32(&failCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(w)
	}

	// Wait for all 100,000 requests to finish
	wg.Wait()
	duration := time.Since(startTime)

	// --- RESULTS ---
	totalRequests := successCount + failCount
	reqPerSec := float64(totalRequests) / duration.Seconds()

	fmt.Println("\n==========================================")
	fmt.Println("🏆 TITANKV BENCHMARK RESULTS 🏆")
	fmt.Println("==========================================")
	fmt.Printf("Total Time:      %v\n", duration)
	fmt.Printf("Total Requests:  %d\n", totalRequests)
	fmt.Printf("Successful:      %d\n", successCount)
	fmt.Printf("Failed:          %d\n", failCount)
	fmt.Printf("Throughput:      %.2f requests/second\n", reqPerSec)
	fmt.Println("==========================================")

	// Verify Data Distribution
	size1 := getDirSize("./data_shard_1")
	size2 := getDirSize("./data_shard_2")
	size3 := getDirSize("./data_shard_3")

	fmt.Println("\n📊 Data Distribution (Consistent Hashing Verification):")
	fmt.Printf("Shard 1 (Port 8081): ~%d bytes on disk\n", size1)
	fmt.Printf("Shard 2 (Port 8082): ~%d bytes on disk\n", size2)
	fmt.Printf("Shard 3 (Port 8083): ~%d bytes on disk\n", size3)
	fmt.Println("If the byte sizes are roughly equal, the Hash Ring is perfectly balanced.")
}

// Helper function to calculate the size of a directory
func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
