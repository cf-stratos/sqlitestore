package sqlitestore

import (
	"database/sql"
	"log"
	"time"
)

var defaultInterval = time.Minute * 5

// SqliteStoreCleanup - Session Store Cleanup helper
type SqliteStoreCleanup struct {
	DbPool *sql.DB,
	TableName string
}

func NewSqliteStoreCleanup(db *sql.DB, tableName string) (*SessionStoreCleanup, error) {
	return &SqliteStoreCleanup{
		DbPool: db,
		TableName: tableName
	}, nil
}

// Cleanup runs a background goroutine every interval that deletes expired
// sessions from the database.
//
// The design is based on https://github.com/yosssi/boltstore
func (m *SqliteStore) Cleanup(interval time.Duration) (chan<- struct{}, <-chan struct{}) {
	if interval <= 0 {
		interval = defaultInterval
	}

	quit, done := make(chan struct{}), make(chan struct{})
	go db.cleanup(interval, quit, done)
	return quit, done
}

// StopCleanup stops the background cleanup from running.
func (m *SqliteStore) StopCleanup(quit chan<- struct{}, done <-chan struct{}) {
	quit <- struct{}{}
	<-done
}

// cleanup deletes expired sessions at set intervals.
func (m *SqliteStore) cleanup(interval time.Duration, quit <-chan struct{}, done chan<- struct{}) {
	ticker := time.NewTicker(interval)

	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-quit:
			// Handle the quit signal.
			done <- struct{}{}
			return
		case <-ticker.C:
			// Delete expired sessions on each tick.
			err := db.deleteExpired()
			if err != nil {
				log.Printf("pgstore: unable to delete expired sessions: %v", err)
			}
		}
	}
}

// deleteExpired deletes expired sessions from the database.
func (m *SqliteStore)  deleteExpired() error {
	var deleteStmt = "DELETE FROM " + m.TableName + " WHERE expires_on < datetime('now')"
	_, err := db.DbPool.Exec(deleteStmt)
	return err
}
