package memdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

// Row represents a database entry
type Row struct {
	Key       string
	Value     interface{}
	CreatedAt int64
}

// DB database object
type DB struct {
	data map[string]Row
	lock sync.RWMutex
	file string
}

// New creates new database
func New(file string) (*DB, error) {
	db := DB{
		data: make(map[string]Row),
		lock: sync.RWMutex{},
		file: file,
	}
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			f, err := os.Create(file)
			if err != nil {
				return nil, err
			}
			f.Close()
		}
	}
	return &db, nil
}

// Begin creates a database transaction
func (db *DB) Begin(writable bool) *Tx {
	tx := &Tx{db, writable}
	tx.lock()
	return tx
}

// Set create or update entries in database
func (db *DB) Set(key string, value interface{}) error {
	tx := db.Begin(true)
	defer tx.unlock()

	return tx.Set(key, value)
}

// Get reads entry
func (db *DB) Get(key string) (Row, error) {
	tx := db.Begin(false)
	defer tx.unlock()

	return tx.Get(key)
}

// Delete delete entry based on key
// Get reads entry
func (db *DB) Delete(key string) error {
	tx := db.Begin(true)
	defer tx.unlock()

	return tx.Delete(key)
}

// Exec performs multiple operation in database
func (db *DB) Exec(fn func(tx *Tx) error) error {
	tx := db.Begin(true)
	defer tx.unlock()

	err := fn(tx)
	if err != nil {
		return fmt.Errorf("transaction failed: %s", err)
	}
	return nil
}

// Save writes data to file storage
func (db *DB) Save() error {
	f, err := os.OpenFile(db.file, os.O_RDWR, 0664)
	if err != nil {
		return err
	}
	defer f.Close()

	db.lock.Lock()
	defer db.lock.Unlock()

	byteValue, err := json.Marshal(db.data)
	if err != nil {
		return err
	}
	_, err = f.Write(byteValue)
	return err
}

// Load loads data from file storage into memory
func (db *DB) Load() error {
	f, err := os.OpenFile(db.file, os.O_RDONLY, 0664)
	if err != nil {
		return err
	}
	defer f.Close()

	db.lock.Lock()
	defer db.lock.Unlock()

	if data, err := ioutil.ReadAll(f); err != nil {
		if err := json.Unmarshal(data, &db.data); err != nil {
			return err
		}
	}
	return nil
}

// Keys returns all availbale keys
func (db *DB) Keys() []string {
	db.lock.Lock()
	defer db.lock.Unlock()
	keys := []string{}
	for key, _ := range db.data {
		keys = append(keys, key)
	}
	return keys
}
