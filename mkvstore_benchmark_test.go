package mkvstore

import (
	"fmt"
	"os" // Import os for temporary file handling
	"testing"
	"time"
)

// setupBenchmarkStore is a helper function to create a new in-memory store for benchmarking.
// We will keep this for benchmarks that don't involve delays or background cleanup.
func setupBenchmarkStore(b *testing.B) *Store {
	// Use ":memory:" for an in-memory database for fast benchmarks
	store, err := Open(":memory:", "benchmark_kv_data")
	if err != nil {
		b.Fatalf("Failed to open in-memory store for benchmark: %v", err)
	}
	// Defer closing the store to clean up after the benchmark finishes
	b.Cleanup(func() {
		store.Close()
	})
	return store
}

// setupBenchmarkFileStore is a helper function to create a new file-based store for benchmarking.
// This is more suitable for benchmarks involving TTL or background cleanup.
func setupBenchmarkFileStore(b *testing.B) *Store {
	// Use a temporary file for this test to ensure a clean start
	tempFile, err := os.CreateTemp("", "mkvstore_benchmark_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file for benchmark: %v", err)
	}
	dbPath := tempFile.Name()
	tempFile.Close() // Close the file handle immediately
	// Defer removal of the temp file
	b.Cleanup(func() {
		os.Remove(dbPath)
	})

	store, err := Open(dbPath, "benchmark_kv_data_file")
	if err != nil {
		b.Fatalf("Failed to open file-based store at %q for benchmark: %v", dbPath, err)
	}
	// Defer closing the store
	b.Cleanup(func() {
		store.Close()
	})

	return store
}

// BenchmarkSet benchmarks the Set operation.
func BenchmarkSet(b *testing.B) {
	store := setupBenchmarkStore(b)

	b.ResetTimer() // Reset timer to exclude setup time

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value, 0) // Set without TTL
		if err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

// BenchmarkSetWithTTL benchmarks the Set operation with a TTL.
func BenchmarkSetWithTTL(b *testing.B) {
	store := setupBenchmarkStore(b)

	b.ResetTimer() // Reset timer to exclude setup time

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := store.Set(key, value, time.Hour) // Set with TTL
		if err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

// BenchmarkGet benchmarks the Get operation on existing keys.
func BenchmarkGet(b *testing.B) {
	store := setupBenchmarkStore(b)

	// Pre-populate the store with keys
	keysToGet := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		store.Set(key, value, 0) // Set without TTL
		keysToGet[i] = key
	}

	b.ResetTimer() // Reset timer to exclude pre-population time

	for i := 0; i < b.N; i++ {
		key := keysToGet[i]
		_, err := store.Get(key)
		if err != nil {
			b.Fatalf("Get failed for key %q: %v", key, err)
		}
	}
}

// BenchmarkGetExpired benchmarks the Get operation on expired keys.
// This tests the performance impact of checking and deleting expired keys during Get.
func BenchmarkGetExpired(b *testing.B) {
	// Use a file-based store for this benchmark due to potential in-memory issues with delays
	store := setupBenchmarkFileStore(b)

	// Pre-populate the store with keys that will expire immediately
	keysToGet := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		// Set with a very short TTL (e.g., 1 nanosecond) to ensure they are expired
		store.Set(key, value, 10*time.Nanosecond)
		keysToGet[i] = key
	}

	// Wait a moment to ensure keys are expired
	time.Sleep(3000 * time.Millisecond)

	b.ResetTimer() // Reset timer to exclude pre-population and wait time

	for i := 0; i < b.N; i++ {
		key := keysToGet[i]
		_, err := store.Get(key)
		// We expect ErrKeyNotFound for expired keys
		if err != ErrKeyNotFound {
			b.Fatalf("Get for expired key %q returned unexpected error: %v", key, err)
		}
	}
}

// BenchmarkDel benchmarks the Del operation.
func BenchmarkDel(b *testing.B) {
	store := setupBenchmarkStore(b)

	// Pre-populate the store with keys to delete
	keysToDelete := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		store.Set(key, value, 0) // Set without TTL
		keysToDelete[i] = key
	}

	b.ResetTimer() // Reset timer to exclude pre-population time

	for i := 0; i < b.N; i++ {
		key := keysToDelete[i]
		err := store.Del(key)
		if err != nil {
			b.Fatalf("Del failed for key %q: %v", key, err)
		}
	}
}

// BenchmarkExists benchmarks the Exists operation.
func BenchmarkExists(b *testing.B) {
	store := setupBenchmarkStore(b)

	// Pre-populate the store with keys
	keysToCheck := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		store.Set(key, value, 0) // Set without TTL
		keysToCheck[i] = key
	}

	b.ResetTimer() // Reset timer to exclude pre-population time

	for i := 0; i < b.N; i++ {
		key := keysToCheck[i]
		_, err := store.Exists(key)
		if err != nil {
			b.Fatalf("Exists failed for key %q: %v", key, err)
		}
	}
}

// BenchmarkTTL benchmarks the TTL operation for keys with TTL.
func BenchmarkTTL(b *testing.B) {
	store := setupBenchmarkStore(b)

	// Pre-populate the store with keys that have TTL
	keysToCheck := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		store.Set(key, value, time.Hour) // Set with TTL
		keysToCheck[i] = key
	}

	b.ResetTimer() // Reset timer to exclude pre-population time

	for i := 0; i < b.N; i++ {
		key := keysToCheck[i]
		_, err := store.TTL(key)
		if err != nil {
			b.Fatalf("TTL failed for key %q: %v", key, err)
		}
	}
}

// BenchmarkKeys benchmarks the Keys operation with a wildcard pattern.
func BenchmarkKeys(b *testing.B) {
	store := setupBenchmarkStore(b)

	// Pre-populate the store with keys
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		store.Set(key, value, 0) // Set without TTL
	}

	b.ResetTimer() // Reset timer to exclude pre-population time

	// Benchmark listing all keys
	_, err := store.Keys("*")
	if err != nil {
		b.Fatalf("Keys('*') failed: %v", err)
	}
}
