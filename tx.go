package memdb

import (
	"fmt"
	"strings"
	"time"
)

type Tx struct {
	db       *DB
	writable bool
}

func (tx *Tx) lock() {
	if tx.writable {
		tx.db.lock.Lock()
	} else {
		tx.db.lock.RLock()
	}
}

func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.lock.Unlock()
	} else {
		tx.db.lock.RUnlock()
	}
}

func (tx *Tx) Set(key string, value interface{}) error {
	row := Row{
		Key:       key,
		Value:     value,
		CreatedAt: time.Now().Unix(),
	}
	tx.db.data[key] = row
	return nil
}

func (tx *Tx) Get(key string) (Row, error) {
	if tx.db.data[key] == (Row{}) {
		return Row{}, fmt.Errorf("key %s not found", key)
	}
	return tx.db.data[key], nil
}

func (tx *Tx) GetPrefix(prefix string) ([]Row, error) {
	var rows = []Row{}
	for key, value := range tx.db.data {
		if strings.HasPrefix(key, prefix) {
			rows = append(rows, value)
		}
	}
	return rows, nil
}

func (tx *Tx) Delete(key string) error {
	if tx.db.data[key] == (Row{}) {
		return fmt.Errorf("key %s not found", key)
	}
	delete(tx.db.data, key)
	return nil
}

func (tx *Tx) DeletePrefix(prefix string) error {
	for key := range tx.db.data {
		if strings.HasPrefix(key, prefix) {
			delete(tx.db.data, key)
		}
	}
	return nil
}
