package mkvstore

import (
	"fmt" // Import fmt for logging in tests
	"os"
	"sort"
	"testing"
	"time"
)

// setupStore is a helper function to create a new in-memory store for testing.
func setupStore(t *testing.T) *Store {
	// Use ":memory:" for an in-memory database for fast tests
	store, err := Open(":memory:", "test_kv_data")
	if err != nil {
		t.Fatalf("Failed to open in-memory store: %v", err)
	}
	// No need to explicitly close in-memory DBs in tests, but good practice
	// defer store.Close() // Deferring in setup might close too early in subtests
	return store
}

// setupFileStore is a helper function to create a new file-based store for testing.
func setupFileStore(t *testing.T) (*Store, string) {
	// Use a temporary file for this test to ensure a clean start
	tempFile, err := os.CreateTemp("", "mkvstore_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	dbPath := tempFile.Name()
	tempFile.Close() // Close the file handle immediately
	// Defer removal of the temp file
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	store, err := Open(dbPath, "test_kv_data_file")
	if err != nil {
		t.Fatalf("Failed to open file-based store at %q: %v", dbPath, err)
	}
	// Defer closing the store
	t.Cleanup(func() {
		store.Close()
	})

	return store, dbPath
}

// TestSetGet tests the basic Set and Get operations without TTL.
func TestSetGet(t *testing.T) {
	store := setupStore(t)
	defer store.Close()

	key := "testkey"
	value := "testvalue"

	// Test setting a key
	err := store.Set(key, value, 0) // 0 TTL
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test getting the key
	gotValue, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if gotValue != value {
		t.Errorf("Get returned wrong value. Expected %q, got %q", value, gotValue)
	}

	// Test getting a non-existent key
	_, err = store.Get("nonexistentkey")
	if err != ErrKeyNotFound {
		t.Errorf("Getting non-existent key should return ErrKeyNotFound, got %v", err)
	}
}

// TestSetGetWithTTL tests setting a key with TTL and verifying expiration.
func TestSetGetWithTTL(t *testing.T) {
	store := setupStore(t)
	defer store.Close()

	key := "expiringkey"
	value := "temporary value"
	ttl := 1 * time.Second // 1 second TTL

	// Test setting a key with TTL
	err := store.Set(key, value, ttl)
	if err != nil {
		t.Fatalf("Set with TTL failed: %v", err)
	}

	// Get the key immediately (should not be expired)
	gotValue, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed immediately after Set with TTL: %v", err)
	}
	if gotValue != value {
		t.Errorf("Get returned wrong value immediately after Set with TTL. Expected %q, got %q", value, gotValue)
	}

	// Wait for the key to expire
	time.Sleep(ttl + 2000*time.Millisecond) // Wait a bit longer than TTL

	// Try to get the key after expiration (should return ErrKeyNotFound)
	_, err = store.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Getting expired key should return ErrKeyNotFound, got %v", err)
	}

	// Test setting an expired key again (should work)
	newValue := "new value after expiration"
	err = store.Set(key, newValue, 0)
	if err != nil {
		t.Fatalf("Set failed after expiration: %v", err)
	}
	gotValue, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get failed after re-setting expired key: %v", err)
	}
	if gotValue != newValue {
		t.Errorf("Get returned wrong value after re-setting expired key. Expected %q, got %q", newValue, gotValue)
	}
}

// TestDel tests the Del operation.
func TestDel(t *testing.T) {
	store := setupStore(t)
	defer store.Close()

	key := "keyToDelete"
	value := "valueToDelete"

	// Set a key
	err := store.Set(key, value, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify it exists
	exists, err := store.Exists(key)
	if err != nil || !exists {
		t.Fatalf("Key %q should exist before deletion", key)
	}

	// Delete the key
	err = store.Del(key)
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}

	// Verify it no longer exists
	exists, err = store.Exists(key)
	if err != nil || exists {
		t.Fatalf("Key %q should not exist after deletion", key)
	}

	// Test deleting a non-existent key (should not return an error)
	err = store.Del("nonexistentkey")
	if err != nil {
		t.Errorf("Deleting non-existent key returned an error: %v", err)
	}
}

// TestExists tests the Exists operation.
func TestExists(t *testing.T) {
	store := setupStore(t)
	defer store.Close()

	key1 := "existskey"
	key2 := "expiringexistskey"
	key3 := "nonexistentkey"

	// Set a key without TTL
	err := store.Set(key1, "value1", 0)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key1, err)
	}

	// Set a key with TTL
	err = store.Set(key2, "value2", 1*time.Second)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key2, err)
	}

	// Check existence of key1 (should exist)
	exists, err := store.Exists(key1)
	if err != nil {
		t.Fatalf("Exists failed for %q: %v", key1, err)
	}
	if !exists {
		t.Errorf("Key %q should exist, but Exists returned false", key1)
	}

	// Check existence of key2 (should exist initially)
	exists, err = store.Exists(key2)
	if err != nil {
		t.Fatalf("Exists failed for %q: %v", key2, err)
	}
	if !exists {
		t.Errorf("Key %q should exist initially, but Exists returned false", key2)
	}

	// Check existence of key3 (should not exist)
	exists, err = store.Exists(key3)
	if err != nil {
		t.Fatalf("Exists failed for %q: %v", key3, err)
	}
	if exists {
		t.Errorf("Key %q should not exist, but Exists returned true", key3)
	}

	// Wait for key2 to expire
	time.Sleep(1*time.Second + 1000*time.Millisecond)

	// Check existence of key2 after expiration (should not exist)
	exists, err = store.Exists(key2)
	if err != nil {
		t.Fatalf("Exists failed for %q after expiration: %v", key2, err)
	}
	if exists {
		t.Errorf("Key %q should not exist after expiration, but Exists returned true", key2)
	}
}

// TestTTL tests the TTL operation.
func TestTTL(t *testing.T) {
	store := setupStore(t)
	defer store.Close()

	key1 := "keyWithTTL"
	key2 := "keyWithoutTTL"
	key3 := "expiringTTLKey"
	key4 := "nonexistentTTLKey"

	// Set a key with TTL
	ttlDuration := 2 * time.Second
	err := store.Set(key1, "value1", ttlDuration)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key1, err)
	}

	// Set a key without TTL
	err = store.Set(key2, "value2", 0)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key2, err)
	}

	// Set a key with short TTL for expiration test
	err = store.Set(key3, "value3", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key3, err)
	}

	// Check TTL for key1 (should be positive and close to ttlDuration)
	ttl, err := store.TTL(key1)
	if err != nil {
		t.Fatalf("TTL failed for %q: %v", key1, err)
	}
	if ttl <= 0 || ttl > ttlDuration {
		t.Errorf("TTL for %q is unexpected: %s (expected > 0 and <= %s)", key1, ttl, ttlDuration)
	}

	// Check TTL for key2 (should be -1, indicating no TTL)
	ttl, err = store.TTL(key2)
	if err != nil {
		t.Fatalf("TTL failed for %q: %v", key2, err)
	}
	if ttl != -1 {
		t.Errorf("TTL for %q should be -1 (no TTL), got %s", key2, ttl)
	}

	// Check TTL for key4 (should return ErrKeyNotFound)
	ttl, err = store.TTL(key4)
	if err != ErrKeyNotFound {
		t.Errorf("TTL for non-existent key %q should return ErrKeyNotFound, got %v", key4, err)
	}
	if ttl != 0 {
		t.Errorf("TTL value for non-existent key %q should be 0, got %s", key4, ttl)
	}

	// Wait for key3 to expire
	time.Sleep(1 * time.Second)

	// Check TTL for key3 after expiration (should return ErrKeyNotFound)
	ttl, err = store.TTL(key3)
	if err != ErrKeyNotFound {
		t.Errorf("TTL for expired key %q should return ErrKeyNotFound, got %v", key3, err)
	}
	if ttl != 0 {
		t.Errorf("TTL value for expired key %q should be 0, got %s", key3, ttl)
	}
}

// TestKeys tests the Keys operation with pattern matching and expiration handling.
func TestKeys(t *testing.T) {
	// Use a file-based store for this test to avoid in-memory database issues
	store, dbPath := setupFileStore(t)
	defer store.Close()

	fmt.Printf("TestKeys using database file: %q\n", dbPath)

	// Set some keys
	store.Set("user:1", "Alice", 0)
	store.Set("user:2", "Bob", 0)
	store.Set("product:101", "Gadget", time.Hour)
	store.Set("tempkey:1", "temporary", 500*time.Millisecond) // Will expire
	store.Set("otherkey", "something else", 0)
	store.Set("key_with%percent", "percent", 0)       // Test keys with special characters
	store.Set("key_with_underscore", "underscore", 0) // Test keys with special characters
	store.Set("another?key", "question", 0)           // Test keys with special characters

	// Wait for tempkey:1 to expire
	time.Sleep(1 * time.Second)

	// Test pattern "*" (all non-expired keys)
	keys, err := store.Keys("*")
	if err != nil {
		t.Fatalf("Keys('*') failed: %v", err)
	}
	expectedKeysAll := []string{
		"user:1", "user:2", "product:101", "otherkey", "key_with%percent", "key_with_underscore", "another?key",
	}
	sort.Strings(keys) // Sort for consistent comparison
	sort.Strings(expectedKeysAll)
	if len(keys) != len(expectedKeysAll) || !sliceEqual(keys, expectedKeysAll) {
		t.Errorf("Keys('*') returned wrong keys. Expected %v, got %v", expectedKeysAll, keys)
	} else {
		fmt.Printf("TestKeys('*') passed. Keys: %v\n", keys)
	}

	// Test pattern "user:*"
	keys, err = store.Keys("user:*")
	if err != nil {
		t.Fatalf("Keys('user:*') failed: %v", err)
	}
	expectedKeysUser := []string{"user:1", "user:2"}
	sort.Strings(keys)
	sort.Strings(expectedKeysUser)
	if len(keys) != len(expectedKeysUser) || !sliceEqual(keys, expectedKeysUser) {
		t.Errorf("Keys('user:*') returned wrong keys. Expected %v, got %v", expectedKeysUser, keys)
	} else {
		fmt.Printf("TestKeys('user:*') passed. Keys: %v\n", keys)
	}

	// Test pattern "*key"
	keys, err = store.Keys("*key")
	if err != nil {
		t.Fatalf("Keys('*key') failed: %v", err)
	}
	expectedKeysEndWithKey := []string{"otherkey", "another?key"}
	sort.Strings(keys)
	sort.Strings(expectedKeysEndWithKey)
	if len(keys) != len(expectedKeysEndWithKey) || !sliceEqual(keys, expectedKeysEndWithKey) {
		t.Errorf("Keys('*key') returned wrong keys. Expected %v, got %v", expectedKeysEndWithKey, keys)
	} else {
		fmt.Printf("TestKeys('*key') passed. Keys: %v\n", keys)
	}

	// Test pattern "product:?" (using Redis-style ?)
	// product:101 has 3 digits, so product:? should not match it.
	// If we had product:1, product:2, etc. they would match.
	// Let's add a key that matches "product:?"
	store.Set("product:A", "Widget A", 0)
	keys, err = store.Keys("product:?")
	if err != nil {
		t.Fatalf("Keys('product:?') failed after adding product:A: %v", err)
	}
	expectedKeysProductSingle := []string{"product:A"}
	sort.Strings(keys)
	sort.Strings(expectedKeysProductSingle)
	if len(keys) != len(expectedKeysProductSingle) || !sliceEqual(keys, expectedKeysProductSingle) {
		t.Errorf("Keys('product:?') returned wrong keys. Expected %v, got %v", expectedKeysProductSingle, keys)
	} else {
		fmt.Printf("TestKeys('product:?') passed. Keys: %v\n", keys)
	}

	// Test pattern "tempkey:*" (should return empty as it's expired)
	keys, err = store.Keys("tempkey:*")
	if err != nil {
		t.Fatalf("Keys('tempkey:*') failed: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("Keys('tempkey:*') should return 0 keys, got %d. Keys: %v", len(keys), keys)
	} else {
		fmt.Printf("TestKeys('tempkey:*') passed (returned 0 keys as expected).\n")
	}

	// Test pattern for keys with literal % and _
	keys, err = store.Keys("key_with%percent")
	if err != nil {
		t.Fatalf("Keys('key_with%%percent') failed: %v", err)
	}
	if len(keys) != 1 || keys[0] != "key_with%percent" {
		t.Errorf("Keys('key_with%%percent') failed. Expected [\"key_with%%percent\"], got %v", keys)
	} else {
		fmt.Printf("TestKeys('key_with%%percent') passed. Keys: %v\n", keys)
	}

	keys, err = store.Keys("key_with_underscore")
	if err != nil {
		t.Fatalf("Keys('key_with_underscore') failed: %v", err)
	}
	if len(keys) != 1 || keys[0] != "key_with_underscore" {
		t.Errorf("Keys('key_with_underscore') failed. Expected [\"key_with_underscore\"], got %v", keys)
	} else {
		fmt.Printf("TestKeys('key_with_underscore') passed. Keys: %v\n", keys)
	}

	keys, err = store.Keys("another?key")
	if err != nil {
		t.Fatalf("Keys('another?key') failed: %v", err)
	}
	if len(keys) != 1 || keys[0] != "another?key" {
		t.Errorf("Keys('another?key') failed. Expected [\"another?key\"], got %v", keys)
	} else {
		fmt.Printf("TestKeys('another?key') passed. Keys: %v\n", keys)
	}

}

// sliceEqual checks if two string slices are equal (order matters after sorting).
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestOpenEmptyTable tests opening the store with a specific table name.
func TestOpenEmptyTable(t *testing.T) {
	// Use a temporary file for this test to ensure a clean start
	tempFile, err := os.CreateTemp("", "mkvstore_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	dbPath := tempFile.Name()
	tempFile.Close()        // Close the file handle immediately
	defer os.Remove(dbPath) // Clean up the temp file

	tableName := "custom_table_name"

	// Open the store with the custom table name
	store, err := Open(dbPath, tableName)
	if err != nil {
		t.Fatalf("Failed to open store with custom table name %q: %v", tableName, err)
	}
	defer store.Close()

	// Verify the store's internal table name
	if store.table != tableName {
		t.Errorf("Store opened with wrong table name. Expected %q, got %q", tableName, store.table)
	}

	// Set a key and verify it works in the custom table
	key := "customkey"
	value := "customvalue"
	err = store.Set(key, value, 0)
	if err != nil {
		t.Fatalf("Set failed in custom table %q: %v", tableName, err)
	}

	gotValue, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed in custom table %q: %v", tableName, err)
	}
	if gotValue != value {
		t.Errorf("Get returned wrong value in custom table %q. Expected %q, got %q", tableName, value, gotValue)
	}

	// Open another store using a different table name in the same DB file (optional, but shows isolation)
	/*
		tableName2 := "another_table"
		store2, err := Open(dbPath, tableName2)
		if err != nil {
			t.Fatalf("Failed to open second store with table name %q: %v", tableName2, err)
		}
		defer store2.Close()

		// Key set in store should not exist in store2
		exists, err := store2.Exists(key)
		if err != nil {
			t.Fatalf("Exists failed in second store %q: %v", tableName2, err)
		}
		if exists {
			t.Errorf("Key %q from %q should not exist in %q", key, tableName, tableName2)
		}
	*/
}

// TestClose tests closing the store.
func TestClose(t *testing.T) {
	store := setupStore(t)

	// Close the store
	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Attempting operations after closing should ideally return an error
	// (though the behavior might depend on the underlying sql.DB implementation)
	// We can't guarantee a specific error from sql.DB after Close, but we can try.
	_, err = store.Get("anykey")
	if err == nil {
		t.Errorf("Get succeeded after closing the store")
	}

	err = store.Set("anykey", "anyvalue", 0)
	if err == nil {
		t.Errorf("Set succeeded after closing the store")
	}
}

// TestRunCleanup tests the background cleanup routine using a file-based database.
func TestRunCleanup(t *testing.T) {
	// Use a file-based store for this test to better simulate persistence and cleanup
	store, dbPath := setupFileStore(t)
	defer store.Close() // Ensure cleanup goroutine is stopped

	fmt.Printf("TestRunCleanup using database file: %q\n", dbPath)

	key1 := "cleanupkey1"
	key2 := "cleanupkey2" // This one should be deleted by cleanup

	// Set a key that should NOT expire soon
	err := store.Set(key1, "value1", 10*time.Second)
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key1, err)
	}

	// Set a key that WILL expire soon
	err = store.Set(key2, "value2", 50*time.Millisecond) // Short TTL
	if err != nil {
		t.Fatalf("Set failed for %q: %v", key2, err)
	}

	fmt.Printf("TestRunCleanup: Set keys %q (10s TTL), %q (50ms TTL)\n", key1, key2)

	// Start cleanup with a short interval
	store.RunCleanup(1000 * time.Millisecond)
	fmt.Printf("TestRunCleanup: Started cleanup every 100ms\n")

	// Wait longer than the key2 TTL and cleanup interval
	waitDuration := 5000 * time.Millisecond
	fmt.Printf("TestRunCleanup: Waiting %s for cleanup...\n", waitDuration)
	time.Sleep(waitDuration)
	fmt.Printf("TestRunCleanup: Wait finished.\n")

	// Check if key1 still exists (should)
	exists1, err := store.Exists(key1)
	if err != nil {
		t.Fatalf("Exists failed for %q: %v", key1, err)
	}
	if !exists1 {
		t.Errorf("Key %q should still exist, but Exists returned false", key1)
	} else {
		fmt.Printf("TestRunCleanup: Key %q still exists (correct).\n", key1)
	}

	// Check if key2 still exists (should NOT)
	exists2, err := store.Exists(key2)
	if err != nil {
		t.Fatalf("Exists failed for %q: %v", key2, err)
	}
	if exists2 {
		t.Errorf("Key %q should have been cleaned up, but Exists returned true", key2)
	} else {
		fmt.Printf("TestRunCleanup: Key %q does not exist (correct, cleaned up).\n", key2)
	}

	// Give cleanup routine a moment to finish logging if needed before test ends
	time.Sleep(100 * time.Millisecond)
}
