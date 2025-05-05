# mkvstore

`mkvstore` is a Go package that provides a simple key-value store with Redis-like features, backed by a SQLite database. It supports basic string key operations, expiration (TTL), existence checks, and key listing with patterns.

## Features

* **Key-Value Storage:** Store string values associated with string keys.

* **Expiration (TTL):** Set a time-to-live for keys, after which they are automatically considered expired.

* **Persistence:** Data is stored in a SQLite database file, providing persistence across application restarts.

* **In-Memory Option:** Use `:memory:` as the database path for a volatile, in-memory store.

* **Table Isolation:** Specify a table name when opening the store to isolate data within the same database file.

* **Background Cleanup:** A configurable background routine to periodically remove expired keys from the database.

* **Redis-like Operations:** Provides `Set`, `Get`, `Del`, `Exists`, `TTL`, and `Keys` methods.

## Limitations

This package is not a full Redis replacement. It has the following limitations:

* Only supports string values. Complex data structures (Lists, Sets, Hashes, etc.) are not implemented.

* Concurrency model is based on Go's `database/sql` and SQLite's capabilities, which differs from Redis.

* `Keys` pattern matching is basic SQL `LIKE` (`%` and `_`) and does not support full Redis glob patterns.

* Expiration check happens on access (`Get`, `Exists`, `TTL`, `Keys`) and via the background cleanup, not guaranteed to be instant upon expiration time.

## Installation

To install the package, use `go get`:

```bash
go get [github.com/yourusername/mkvstore](https://www.google.com/url?sa=E&source=gmail&q=https://github.com/yourusername/mkvstore) \# Replace with your actual module path
```

You will also need the SQLite driver:

```bash
go get [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)

```

## Usage

Here's a basic example demonstrating how to use the `mkvstore`:

```go
package main

import (
	"fmt"
	"log"
	"time"

	"[github.com/yourusername/mkvstore](https://github.com/yourusername/mkvstore)" // Replace with your actual module path
)

func main() {
	// Open the store (use ":memory:" for in-memory, or a file path for persistent)
	// Specify the database file path and the table name.
	store, err := mkvstore.Open("./mycache.db", "kv_data")
	if err != nil {
		log.Fatalf("Failed to open MKVStore: %v", err)
	}
	defer store.Close() // Ensure the store is closed when main exits

	// Start background cleanup (optional but recommended for persistence)
	// Clean up expired keys every 5 minutes.
	store.RunCleanup(5 * time.Minute)

	fmt.Println("Store opened successfully.")

	// --- Basic Set and Get ---
	fmt.Println("\n--- Basic Set/Get ---")
	err = store.Set("mykey", "hello", 0) // Set without TTL (0 or negative duration)
	if err != nil {
		log.Printf("Error setting mykey: %v", err)
	}
	value, err := store.Get("mykey")
	if err != nil {
		log.Printf("Error getting mykey: %v", err)
	} else {
		fmt.Printf("Get mykey: %s\n", value)
	}

	// --- Set with TTL ---
	fmt.Println("\n--- Set with TTL ---")
	err = store.Set("expiringkey", "this will expire", 5 * time.Second) // Set with 5s TTL
	if err != nil {
		log.Printf("Error setting expiringkey: %v", err)
	}
	fmt.Println("Set expiringkey with 5s TTL")

	// Get TTL
	ttl, err := store.TTL("expiringkey")
	if err != nil {
		log.Printf("Error getting TTL for expiringkey: %v", err)
	} else if ttl == -1 {
		fmt.Println("TTL for expiringkey: No TTL")
	} else if ttl == 0 {
         fmt.Println("TTL for expiringkey: Key not found or expired")
    } else {
		fmt.Printf("TTL for expiringkey: %s\n", ttl)
	}

	// Wait for expiration (for demonstration)
	fmt.Println("Waiting 6 seconds for expiringkey to expire...")
	time.Sleep(6 * time.Second)

	// Try to get after expiration
	value, err = store.Get("expiringkey")
	if err != nil {
		fmt.Printf("Get expiringkey after expiration: %v\n", err) // Should be ErrKeyNotFound
	} else {
		fmt.Printf("Get expiringkey after expiration: %s (Should not happen)\n", value)
	}

	// --- Exists ---
	fmt.Println("\n--- Exists ---")
	exists, err := store.Exists("mykey") // Should be false after deletion below
	if err != nil {
		log.Printf("Error checking existence of mykey: %v", err)
	} else {
		fmt.Printf("Exists mykey: %t\n", exists)
	}
	exists, err = store.Exists("nonexistentkey")
	if err != nil {
		log.Printf("Error checking existence of nonexistentkey: %v", err)
	} else {
		fmt.Printf("Exists nonexistentkey: %t\n", exists) // Should be false
	}

	// --- Delete ---
	fmt.Println("\n--- Delete ---")
	err = store.Del("mykey")
	if err != nil {
		log.Printf("Error deleting mykey: %v", err)
	}
	fmt.Println("Deleted mykey")

	value, err = store.Get("mykey")
	if err != nil {
		fmt.Printf("Get mykey after deletion: %v\n", err) // Should be ErrKeyNotFound
	} else {
		fmt.Printf("Get mykey after deletion: %s (Should not happen)\n", value)
	}

	// --- Keys ---
	fmt.Println("\n--- Keys ---")
	store.Set("user:1", "Alice", 0)
	store.Set("user:2", "Bob", 0)
	store.Set("product:101", "Gadget", time.Hour)
	store.Set("tempkey:1", "temporary", 1*time.Second) // Will expire

	time.Sleep(2 * time.Second) // Let tempkey:1 expire

	keys, err := store.Keys("*") // All non-expired keys
	if err != nil {
		log.Printf("Error listing keys (*): %v", err)
	} else {
		fmt.Printf("Keys (*): %v\n", keys) // Should list user:1, user:2, product:101
	}

	keys, err = store.Keys("user:*") // Keys starting with user:
	if err != nil {
		log.Printf("Error listing keys (user:*): %v", err)
	} else {
		fmt.Printf("Keys (user:*): %v\n", keys) // Should list user:1, user:2
	}

	keys, err = store.Keys("product:%") // Using % for LIKE pattern
	if err != nil {
		log.Printf("Error listing keys (product:%%): %v", err)
	} else {
		fmt.Printf("Keys (product:%%): %v\n", keys) // Should list product:101
	}

	fmt.Println("\n--- Background cleanup will run periodically ---")
	// The background cleanup routine will automatically delete expired keys
	// even if they are not accessed.

	// Keep the main goroutine alive briefly to allow cleanup to run
	time.Sleep(2 * time.Second)
}
```

## Running Tests

Navigate to the package directory (`mkvstore`) and run the tests:

```bash
go test .

```

## Running Benchmarks

Navigate to the package directory (`mkvstore`) and run the benchmarks:

```bash
go test -bench=. .

```

Benchmark results will vary depending on your system.

## Contributing

Contributions are welcome\! Please open issues or submit pull requests.

## License

[Apache 2.0]
