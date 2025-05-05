package mkvstore

import (
	"context"
	"database/sql"
	"errors" // Import errors package explicitly
	"fmt"
	"os"
	"strings" // Import strings for quoting the table name
	"time"

	_ "github.com/mattn/go-sqlite3" // Import the SQLite driver
)

// Store represents the key-value store backed by SQLite.
type Store struct {
	db    *sql.DB
	table string // Store the table name here
	// Context and cancel function for background cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

// Open opens a new connection to the SQLite database and initializes the schema
// using the specified table name.
// dbPath is the path to the SQLite database file. Use ":memory:" for an in-memory database.
// table is the name of the table to use within the database.
func Open(dbPath string, table string) (*Store, error) {
	if table == "" {
		return nil, errors.New("table name cannot be empty")
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Ping to ensure the connection is valid
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &Store{
		db:    db,
		table: table,
	}

	// Create the table if it doesn't exist
	// Use store.quoteTable to safely include the table name in SQL
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		key TEXT PRIMARY KEY,
		value TEXT,
		type TEXT NOT NULL DEFAULT 'string', -- 'string', 'list', 'hash', etc. (currently only 'string' supported)
		expires_at INTEGER NULL -- Unix timestamp, NULL for no expiration
	);`, store.quoteTable())

	if _, err = db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table %q: %w", table, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.ctx = ctx
	store.cancel = cancel

	return store, nil
}

// quoteTable returns the table name safely quoted for SQL.
func (s *Store) quoteTable() string {
	// Simple quoting for SQLite. For more complex scenarios,
	// you might need a more robust quoting function.
	return "\"" + strings.ReplaceAll(s.table, "\"", "\"\"") + "\""
}

// Close closes the database connection and stops any background routines.
func (s *Store) Close() error {
	// Signal background routines to stop
	if s.cancel != nil {
		s.cancel()
	}

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Set sets the string value of a key. If the key already exists, it is overwritten.
// ttl is the time duration for the key to live. Use 0 or negative for no expiration.
func (s *Store) Set(key string, value string, ttl time.Duration) error {
	var expiresAt interface{} // Use interface{} to allow for NULL
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).Unix()
	} else {
		expiresAt = nil // Set to NULL in the database
	}

	// Use fmt.Sprintf to dynamically build the SQL with the table name
	setSQL := fmt.Sprintf(`INSERT OR REPLACE INTO %s (key, value, type, expires_at) VALUES (?, ?, 'string', ?);`, s.quoteTable())

	_, err := s.db.Exec(setSQL, key, value, expiresAt)
	if err != nil {
		return fmt.Errorf("failed to set key %q in table %q: %w", key, s.table, err)
	}
	return nil
}

// Get retrieves the string value of a key.
// Returns ErrKeyNotFound if the key does not exist, is expired, or is not a string.
func (s *Store) Get(key string) (string, error) {
	var value string
	var keyType string
	var expiresAt sql.NullInt64 // Use sql.NullInt64 to handle NULL

	// Use fmt.Sprintf to dynamically build the SQL with the table name
	getSQL := fmt.Sprintf(`SELECT value, type, expires_at FROM %s WHERE key = ?;`, s.quoteTable())

	row := s.db.QueryRow(getSQL, key)
	err := row.Scan(&value, &keyType, &expiresAt)

	if err == sql.ErrNoRows {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to get key %q from table %q: %w", key, s.table, err)
	}

	// Check the key type (currently only 'string' is supported for Get)
	if keyType != "string" {
		// Optionally delete if wrong type? Redis doesn't delete on WRONGTYPE.
		// Let's return ErrWrongType for now.
		return "", ErrWrongType
	}

	// Check for expiration
	if expiresAt.Valid {
		if time.Now().Unix() > expiresAt.Int64 {
			// Key is expired, delete it and return not found
			// Use a goroutine to avoid blocking the Get operation
			go s.Del(key) // Delete asynchronously, ignore error here
			return "", ErrKeyNotFound
		}
	}

	return value, nil
}

// Del deletes a key. It returns nil if the key was deleted or did not exist.
func (s *Store) Del(key string) error {
	// Use fmt.Sprintf to dynamically build the SQL with the table name
	delSQL := fmt.Sprintf(`DELETE FROM %s WHERE key = ?;`, s.quoteTable())
	_, err := s.db.Exec(delSQL, key)
	if err != nil {
		return fmt.Errorf("failed to delete key %q from table %q: %w", key, s.table, err)
	}
	return nil // Deleting a non-existent key is not an error in Redis
}

// Exists checks if a key exists and is not expired.
// Returns true if the key exists and is valid, false otherwise.
func (s *Store) Exists(key string) (bool, error) {
	var keyType string
	var expiresAt sql.NullInt64

	// Use fmt.Sprintf to dynamically build the SQL with the table name
	existsSQL := fmt.Sprintf(`SELECT type, expires_at FROM %s WHERE key = ?;`, s.quoteTable())

	row := s.db.QueryRow(existsSQL, key)
	err := row.Scan(&keyType, &expiresAt)

	if err == sql.ErrNoRows {
		return false, nil // Key does not exist
	}
	if err != nil {
		return false, fmt.Errorf("failed to check existence of key %q in table %q: %w", key, s.table, err)
	}

	// Check for expiration
	if expiresAt.Valid {
		if time.Now().Unix() > expiresAt.Int64 {
			// Key is expired, delete it and return false
			// Use a goroutine to avoid blocking the Exists operation
			go s.Del(key) // Delete asynchronously, ignore error here
			return false, nil
		}
	}

	// Key exists and is not expired
	return true, nil
}

// TTL returns the remaining time to live of a key.
// Returns:
// - time.Duration remaining time if the key has a TTL and is not expired.
// - -1 and nil error if the key exists but has no associated TTL.
// - 0 and ErrKeyNotFound if the key does not exist or is expired.
// - 0 and ErrWrongType if the key exists but is not a string.
//
// Note: Redis returns specific integer values (-1 for no TTL, -2 for not found/expired).
// We map -1 to a non-zero Duration and nil error, 0+ Duration to remaining TTL,
// and 0 Duration with ErrKeyNotFound for not found/expired.
func (s *Store) TTL(key string) (time.Duration, error) {
	var expiresAt sql.NullInt64
	var keyType string

	// Use fmt.Sprintf to dynamically build the SQL with the table name
	ttlSQL := fmt.Sprintf(`SELECT expires_at, type FROM %s WHERE key = ?;`, s.quoteTable())

	row := s.db.QueryRow(ttlSQL, key)
	err := row.Scan(&expiresAt, &keyType)

	if err == sql.ErrNoRows {
		return 0, ErrKeyNotFound // Key does not exist
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get TTL for key %q in table %q: %w", key, s.table, err)
	}

	// Check the key type (optional, but good practice if adding other types)
	// Redis TTL works on any key type, but PTTL returns specific values.
	// Let's return ErrWrongType if it's not 'string' for clarity in this K/V store.
	if keyType != "string" {
		return 0, ErrWrongType
	}

	if !expiresAt.Valid {
		return -1, nil // Key exists but has no TTL (returns -1 like Redis PTTL)
	}

	expiryTime := time.Unix(expiresAt.Int64, 0)
	now := time.Now()

	if expiryTime.Before(now) {
		// Key is expired, delete it and return not found
		// Use a goroutine to avoid blocking the TTL operation
		go s.Del(key) // Delete asynchronously, ignore error here
		return 0, ErrKeyNotFound
	}

	return expiryTime.Sub(now), nil // Remaining duration
}

// globToSQLLike converts a Redis-style glob pattern to a SQL LIKE pattern.
// It handles '*' -> '%', '?' -> '_', and escapes '%' and '_' literals.
func globToSQLLike(glob string) string {
	var result strings.Builder
	result.Grow(len(glob) * 2) // Estimate capacity

	replacer := strings.NewReplacer(
		`%`, `\%`, // Escape literal %
		`_`, `\_`, // Escape literal _
		`*`, `%`, // Convert glob * to SQL %
		`?`, `_`, // Convert glob ? to SQL _
	)

	// Replace glob characters and escape SQL special characters
	result.WriteString(replacer.Replace(glob))

	return result.String()
}

// Keys returns all keys matching the pattern.
// Pattern supports Redis-style glob patterns: '*' (any sequence), '?' (any single character).
// Expired keys are deleted and not included in the results.
// Only string keys are returned (adjust if other types are added).
func (s *Store) Keys(pattern string) ([]string, error) {
	// Convert Redis glob pattern to SQL LIKE pattern
	sqlPattern := globToSQLLike(pattern)

	// Use fmt.Sprintf to dynamically build the SQL with the table name
	// Add ESCAPE '\' to the LIKE clause to correctly handle escaped % and _
	keysSQL := fmt.Sprintf(`SELECT key, type, expires_at FROM %s WHERE key LIKE ? ESCAPE '\';`, s.quoteTable())

	rows, err := s.db.Query(keysSQL, sqlPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to query keys with pattern %q (SQL LIKE %q) from table %q: %w", pattern, sqlPattern, s.table, err)
	}
	defer rows.Close()

	var keys []string
	var keysToDelete []string // Collect expired keys to delete later

	for rows.Next() {
		var key string
		var keyType string
		var expiresAt sql.NullInt64

		if err := rows.Scan(&key, &keyType, &expiresAt); err != nil {
			// Log the error and continue to the next row
			fmt.Fprintf(os.Stderr, "mkvstore: error scanning key row in table %q: %v\n", s.table, err)
			continue
		}

		// Check type (only return strings for now)
		if keyType != "string" {
			continue
		}

		// Check expiration
		if expiresAt.Valid && time.Now().Unix() > expiresAt.Int64 {
			keysToDelete = append(keysToDelete, key)
			continue // Skip expired keys
		}

		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through keys rows in table %q: %w", s.table, err)
	}

	// Delete collected expired keys outside the scan loop
	// Use goroutines for asynchronous deletion to not block the Keys operation
	for _, key := range keysToDelete {
		go s.Del(key) // Delete asynchronously, ignore error
	}

	return keys, nil
}
