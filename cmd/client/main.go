package main

import (
	"fmt"
	"log"

	"github.com/NonOrdinary/titankv/internal/server"
)

func main() {
	fmt.Println("Connecting to TitanKV...")

	client, err := server.NewClient("127.0.0.1:8080")
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer client.Close()

	key := "demo_key"
	value := []byte("Hello, TitanKV is working!")

	//Put test(Create)
	fmt.Printf("PUT %s -> %s\n", key, string(value))
	if err := client.Put(key, value); err != nil {
		log.Fatalf("PUT failed: %v", err)
	}

	// GET(Read)
	fmt.Printf("GET %s\n", key)
	val, exists, err := client.Get(key)
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	if !exists {
		log.Fatalf("Expected key to exist, but it was not found.")
	}
	fmt.Printf("Success! Retrieved: %s\n", string(val))

	// DELETE(Delete)
	fmt.Printf("DELETE %s\n", key)
	if err := client.Delete(key); err != nil {
		log.Fatalf("DELETE failed: %v", err)
	}

	// 4. Test GET (Verify Deletion)
	fmt.Printf("GET %s (Verifying deletion)\n", key)
	_, exists, err = client.Get(key)
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	if exists {
		log.Fatalf("Key should have been deleted, but it still exists!")
	}
	fmt.Println("Success! Key was correctly reported as not found.")

	//update
	// new_value := []byte("This is the new key")
	// fmt.Printf("PUT %s -> %s\n", key, string(new_value))
	// if err := client.Put(key, value); err != nil {
	// 	log.Fatalf("PUT failed: %v", err)
	// }
	//get the updated value

	// // GET(Read)
	// fmt.Printf("GET %s\n", key)
	// new_val, new_exists, new_err := client.Get(key)
	// if new_err != nil {
	// 	log.Fatalf("GET failed: %v", err)
	// }
	// if !new_exists {
	// 	log.Fatalf("Expected key to exist, but it was not found.")
	// }
	// fmt.Printf("Success! Retrieved: %s\n", string(new_val))

}
