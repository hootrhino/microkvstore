package mkvstore

import (
	"fmt"
	"os"
	"time"
)

// RunCleanup starts a background goroutine to periodically delete expired keys.
// Call this after opening the store. The routine stops when Store.Close() is called.
// interval is the frequency of the cleanup runs.
func (s *Store) RunCleanup(interval time.Duration) {
	if s.db == nil {
		fmt.Println("mkvstore: cleanup cannot start, database connection is nil")
		return
	}

	// Ensure interval is positive
	if interval <= 0 {
		fmt.Println("mkvstore: cleanup interval must be positive, cleanup not started")
		return
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		fmt.Printf("mkvstore: starting background cleanup for table %q every %s\n", s.table, interval)

		// Dynamically build the SQL statement for cleanup
		deleteExpiredSQL := fmt.Sprintf(`DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < ?;`, s.quoteTable())

		for {
			select {
			case <-s.ctx.Done():
				fmt.Printf("mkvstore: background cleanup for table %q stopped\n", s.table)
				return // Context cancelled, stop the goroutine
			case <-ticker.C:
				now := time.Now().Unix()
				result, err := s.db.Exec(deleteExpiredSQL, now)
				if err != nil {
					fmt.Fprintf(os.Stderr, "mkvstore: background cleanup error for table %q: %v\n", s.table, err)
					continue // Continue with the next tick
				}
				rowsAffected, _ := result.RowsAffected()
				if rowsAffected > 0 {
					fmt.Printf("mkvstore: background cleanup deleted %d expired keys from table %q\n", rowsAffected, s.table)
				}
			}
		}
	}()
}
