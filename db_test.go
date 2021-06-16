package memdb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	db, err := New("dbtest.json")
	assert.NoError(t, err)
	assert.NotNil(t, db)
}

func TestSet(t *testing.T) {
	db, _ := New("dbtest.json")
	err := db.Set("foo", "bar")
	assert.Nil(t, err)
}

func TestConcurrency(t *testing.T) {
	db, _ := New("dbtest.json")
	db.Set("count", 5)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			db.Exec(func(tx *Tx) error {
				r, _ := tx.Get("count")
				v, _ := r.Value.(int)
				tx.Set("count", v+1)
				return nil
			})
			wg.Done()
		}()
	}
	wg.Wait()
	r, _ := db.Get("count")
	assert.Equal(t, r.Value, 105)
}

func TestDelete(t *testing.T) {
	db, _ := New("dbtest.json")
	db.Set("foo", 5)
	err := db.Delete("foo")
	assert.NoError(t, err)
	_, err = db.Get("foo")
	assert.NotNil(t, err)
}

func TestSaveAndLoad(t *testing.T) {
	db, _ := New("dbtest.json")
	db.Set("foo", 100)
	err := db.Save()
	assert.NoError(t, err)

	err = db.Load()
	assert.NoError(t, err)

	r, _ := db.Get("foo")
	assert.Equal(t, r.Value, 100)
}
