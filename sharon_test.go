package sharon_test

import (
	"os"
	"testing"

	"github.com/ehebe/sharon"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func setupDB(t *testing.T) *sharon.DB {
	_ = os.RemoveAll("testdb")
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := sharon.Open("testdb", o)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	return db
}

func TestHsetHget(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	name := "mytest"
	key := []byte("mykey")
	value := []byte("hello")

	err := db.Hset(name, key, value)
	if err != nil {
		t.Fatalf("Hset failed: %v", err)
	}

	rs := db.Hget(name, key)
	if !rs.OK() {
		t.Fatalf("Hget failed: %s", rs.State)
	}
	if got := rs.Bytes(); string(got) != string(value) {
		t.Errorf("expected %s, got %s", value, got)
	}
}

func TestHgetNotFound(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	rs := db.Hget("nonexist", []byte("nokey"))
	if !rs.NotFound() {
		t.Errorf("expected NotFound, got %s", rs.State)
	}
}

func TestHIncr(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	name := "counter"
	key := []byte("cnt")

	val, err := db.Hincr(name, key, 5)
	if err != nil {
		t.Fatalf("Hincr failed: %v", err)
	}
	if val != 5 {
		t.Errorf("expected 5, got %d", val)
	}

	val, err = db.Hincr(name, key, -2)
	if err != nil {
		t.Fatalf("Hincr -2 failed: %v", err)
	}
	if val != 3 {
		t.Errorf("expected 3, got %d", val)
	}
}

func TestZsetZget(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	name := "myzset"
	key := []byte("member1")

	err := db.Zset(name, key, 100)
	if err != nil {
		t.Fatalf("Zset failed: %v", err)
	}

	rs := db.Zget(name, key)
	if rs != 100 {
		t.Errorf("expected 100, got %d", rs)
	}
}
