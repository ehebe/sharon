package sharon

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"strconv"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	replyOK              = "ok"
	replyNotFound        = "leveldb: not found"
	replyError           = "error"
	scoreByteLen         = 8
	scoreMin      uint64 = 0
	scoreMax      uint64 = math.MaxUint64
)

var (
	hashPrefix     = []byte{30}
	zetKeyPrefix   = []byte{31}
	zetScorePrefix = []byte{29}
	splitChar      = []byte{28}
)

type (
	BS []byte
	// DB embeds a leveldb.DB.
	DB struct {
		*leveldb.DB
	}

	// Reply a holder for a Entry list of a hashmap.
	Reply struct {
		State string
		Data  []BS
	}

	// Entry a key-value pair.
	Entry struct {
		Key, Value BS
	}
)

// Open creates/opens a DB at specified path, and returns a DB enclosing the same.
func Open(dbPath string, o *opt.Options) (*DB, error) {
	database, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		if errors.IsCorrupted(err) {
			if database, err = leveldb.RecoverFile(dbPath, o); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &DB{database}, nil
}

// Close closes the DB.
func (db *DB) Close() error {
	return db.DB.Close()
}

// Hset set the byte value in argument as value of the key of a hashmap.
func (db *DB) Hset(name string, key, val []byte) error {
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key)
	return db.Put(realKey, val, nil)
}

// Hget get the value related to the specified key of a hashmap.
func (db *DB) Hget(name string, key []byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key)
	val, err := db.Get(realKey, nil)
	if err != nil {
		r.State = err.Error()
		return r
	}
	r.State = replyOK
	r.Data = append(r.Data, val)
	return r
}

// Hmset set multiple key-value pairs of a hashmap in one method call.
func (db *DB) Hmset(name string, kvs ...[]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	keyPrefix := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)
	batch := new(leveldb.Batch)
	for i := 0; i < (len(kvs) - 1); i += 2 {
		batch.Put(Bconcat(keyPrefix, kvs[i]), kvs[i+1])
	}
	return db.Write(batch, nil)
}

// Hmget get the values related to the specified multiple keys of a hashmap.
func (db *DB) Hmget(name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	keyPrefix := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)
	for _, key := range keys {
		val, err := db.Get(Bconcat(keyPrefix, key), nil)
		if err != nil {
			continue
		}
		r.Data = append(r.Data, key, val)
	}
	if len(r.Data) > 0 {
		r.State = replyOK
	}
	return r
}

// Hincr increment the number stored at key in a hashmap by step.
func (db *DB) Hincr(name string, key []byte, step int64) (newNum uint64, err error) {
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key)
	var oldNum uint64
	var val []byte
	val, err = db.Get(realKey, nil)
	if err == nil {
		oldNum = BytesToUint64(val)
	}
	if step > 0 {
		if (scoreMax - uint64(step)) < oldNum {
			err = errors.New("overflow number")
			return
		}
		newNum = oldNum + uint64(step)
	} else {
		if uint64(-step) > oldNum {
			err = errors.New("overflow number")
			return
		}
		newNum = oldNum - uint64(-step)
	}

	err = db.Put(realKey, Uint64ToBytes(newNum), nil)
	if err != nil {
		newNum = 0
		return
	}
	return
}

// HgetInt get the value related to the specified key of a hashmap.
func (db *DB) HgetInt(name string, key []byte) uint64 {
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key)
	val, err := db.Get(realKey, nil)
	if err != nil {
		return 0
	}
	return BytesToUint64(val)
}

func (db *DB) HhasKey(name string, key []byte) bool {
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key)
	has, err := db.Has(realKey, nil)
	if err != nil {
		return false
	}
	return has
}

// Hdel delete specified key of a hashmap.
func (db *DB) Hdel(name string, key []byte) error {
	return db.Delete(Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, key), nil)
}

// Hmdel delete specified multiple keys of a hashmap.
func (db *DB) Hmdel(name string, keys [][]byte) error {
	batch := new(leveldb.Batch)
	keyPrefix := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)
	for _, key := range keys {
		batch.Delete(Bconcat(keyPrefix, key))
	}
	return db.Write(batch, nil)
}

// HdelBucket delete all keys in a hashmap.
func (db *DB) HdelBucket(name string) error {
	batch := new(leveldb.Batch)
	iter := db.NewIterator(util.BytesPrefix(Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	return db.Write(batch, nil)
}

// Hscan list key-value pairs of a hashmap with keys in range (key_start, key_end].
func (db *DB) Hscan(name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}
	keyPrefix := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)
	realKey := Bconcat(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []BS{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

func (db *DB) Hprefix(name string, prefix []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}
	realKey := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar, prefix) // keyPrefix
	keyPrefixLen := len(realKey)
	n := 0
	sliceRange := util.BytesPrefix(realKey)
	if len(realKey) > keyPrefixLen {
		sliceRange.Start = realKey
	} else {
		realKey = sliceRange.Start
	}
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyPrefixLen:]...),
				append([]byte{}, iter.Value()...),
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []BS{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

// Hrscan list key-value pairs of a hashmap with keys in range (key_start, key_end], in reverse order.
func (db *DB) Hrscan(name string, keyStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}
	keyPrefix := Bconcat(hashPrefix, StringToBytesNoCopy(name), splitChar)
	realKey := Bconcat(keyPrefix, keyStart)
	keyPrefixLen := len(keyPrefix)
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(realKey) > keyPrefixLen {
		sliceRange.Limit = realKey
	} else {
		realKey = sliceRange.Limit
	}
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		r.Data = append(r.Data,
			append([]byte{}, iter.Key()[keyPrefixLen:]...),
			append([]byte{}, iter.Value()...),
		)
		n++
		if n == limit {
			break
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []BS{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

// Zset set the score of the key of a zset.
func (db *DB) Zset(name string, key []byte, val uint64) error {
	nameB := StringToBytesNoCopy(name)
	score := Uint64ToBytes(val)
	keyScore := Bconcat(zetScorePrefix, nameB, splitChar, key)                    // key / score
	newScoreKey := Bconcat(zetKeyPrefix, nameB, splitChar, score, splitChar, key) // name+score+key / nil

	oldScore, _ := db.Get(keyScore, nil)
	if !bytes.Equal(oldScore, score) {
		batch := new(leveldb.Batch)
		batch.Put(keyScore, score)
		batch.Put(newScoreKey, nil)
		batch.Delete(Bconcat(zetKeyPrefix, nameB, splitChar, oldScore, splitChar, key))
		return db.Write(batch, nil)
	}
	return nil
}

// Zincr increment the number stored at key in a zset by step.
func (db *DB) Zincr(name string, key []byte, step int64) (uint64, error) {
	nameB := StringToBytesNoCopy(name)
	keyScore := Bconcat(zetScorePrefix, nameB, splitChar, key) // key / score

	score := db.Zget(name, key)       // get old score
	oldScoreB := Uint64ToBytes(score) // old score byte
	if step > 0 {
		if (scoreMax - uint64(step)) < score {
			return 0, errors.New("overflow number")
		}
		score += uint64(step)
	} else {
		if uint64(-step) > score {
			return 0, errors.New("overflow number")
		}
		score -= uint64(-step)
	}

	newScoreB := Uint64ToBytes(score)

	batch := new(leveldb.Batch)
	batch.Put(keyScore, newScoreB)
	batch.Put(Bconcat(zetKeyPrefix, nameB, splitChar, newScoreB, splitChar, key), nil)
	batch.Delete(Bconcat(zetKeyPrefix, nameB, splitChar, oldScoreB, splitChar, key))
	err := db.Write(batch, nil)
	if err != nil {
		return 0, err
	}
	return score, nil
}

// Zget get the score related to the specified key of a zset.
func (db *DB) Zget(name string, key []byte) uint64 {
	val, err := db.Get(Bconcat(zetScorePrefix, StringToBytesNoCopy(name), splitChar, key), nil)
	if err != nil {
		return 0
	}
	return BytesToUint64(val)
}

func (db *DB) ZhasKey(name string, key []byte) bool {
	has, err := db.Has(Bconcat(zetScorePrefix, StringToBytesNoCopy(name), splitChar, key), nil)
	if err != nil {
		return false
	}
	return has
}

// Zdel delete specified key of a zset.
func (db *DB) Zdel(name string, key []byte) error {
	nameB := StringToBytesNoCopy(name)
	keyScore := Bconcat(zetScorePrefix, nameB, splitChar, key) // key / score

	oldScore, err := db.Get(keyScore, nil)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Delete(keyScore)
	batch.Delete(Bconcat(zetKeyPrefix, nameB, splitChar, oldScore, splitChar, key))
	return db.Write(batch, nil)
}

// ZdelBucket delete all keys in a zset.
func (db *DB) ZdelBucket(name string) error {
	nameB := StringToBytesNoCopy(name)
	batch := new(leveldb.Batch)

	iter := db.NewIterator(util.BytesPrefix(Bconcat(zetScorePrefix, nameB, splitChar)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}

	iter = db.NewIterator(util.BytesPrefix(Bconcat(zetKeyPrefix, nameB, splitChar)), nil)
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}

	return db.Write(batch, nil)
}

// Zmset et multiple key-score pairs of a zset in one method call.
func (db *DB) Zmset(name string, kvs [][]byte) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("kvs len must is an even number")
	}
	nameB := StringToBytesNoCopy(name)

	keyPrefix1 := Bconcat(zetScorePrefix, nameB, splitChar)
	keyPrefix2 := Bconcat(zetKeyPrefix, nameB, splitChar)

	batch := new(leveldb.Batch)
	for i := 0; i < (len(kvs) - 1); i += 2 {
		key, score := kvs[i], kvs[i+1]

		keyScore := Bconcat(keyPrefix1, key)                      // key / score
		newScoreKey := Bconcat(keyPrefix2, score, splitChar, key) // name+score+key / nil

		oldScore, _ := db.Get(keyScore, nil)
		if !bytes.Equal(oldScore, score) {
			batch.Put(keyScore, score)
			batch.Put(newScoreKey, nil)
			batch.Delete(Bconcat(keyPrefix2, oldScore, splitChar, key))
		}
	}
	return db.Write(batch, nil)
}

// Zmget get the values related to the specified multiple keys of a zset.
func (db *DB) Zmget(name string, keys [][]byte) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	keyPrefix := Bconcat(zetScorePrefix, StringToBytesNoCopy(name), splitChar)
	for _, key := range keys {
		val, err := db.Get(Bconcat(keyPrefix, key), nil)
		if err != nil {
			continue
		}
		r.Data = append(r.Data, key, val)
	}
	if len(r.Data) > 0 {
		r.State = replyOK
	}
	return r
}

// Zmdel delete specified multiple keys of a zset.
func (db *DB) Zmdel(name string, keys [][]byte) error {
	nameB := StringToBytesNoCopy(name)
	batch := new(leveldb.Batch)
	keyPrefix := Bconcat(zetScorePrefix, nameB, splitChar)
	keyPrefix2 := Bconcat(zetKeyPrefix, nameB, splitChar)
	for _, key := range keys {
		keyScore := Bconcat(keyPrefix, key) // key / score
		oldScore, err := db.Get(keyScore, nil)
		if err != nil {
			continue
		}
		batch.Delete(keyScore)
		batch.Delete(Bconcat(keyPrefix2, oldScore, splitChar, key))
	}
	return db.Write(batch, nil)
}

// Zscan list key-score pairs in a zset, where key-score in range (key_start+score_start, score_end].
func (db *DB) Zscan(name string, keyStart, scoreStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	if len(scoreStart) == 0 {
		scoreStart = Uint64ToBytes(scoreMin)
	}

	keyPrefix := Bconcat(zetKeyPrefix, StringToBytesNoCopy(name), splitChar)
	realKey := Bconcat(keyPrefix, scoreStart, splitChar, keyStart)
	// zetKeyPrefix+name+splitChar+score+splitChar+key
	// split by splitChar: [zetKeyPrefix+name, score, key, ...]
	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen
	keyBeginIndex := scoreBeginIndex + scoreByteLen + 1
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(Bconcat(keyPrefix, scoreStart, splitChar)).Limit
	}
	sliceRange.Start = realKey
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if bytes.Compare(realKey, iter.Key()) == -1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyBeginIndex:]...),                // key
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...), // score
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []BS{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

// Zrscan list key-score pairs of a zset, in reverse order.
func (db *DB) Zrscan(name string, keyStart, scoreStart []byte, limit int) *Reply {
	r := &Reply{
		State: replyError,
		Data:  []BS{},
	}

	if len(scoreStart) == 0 {
		scoreStart = Uint64ToBytes(scoreMax)
	}

	keyPrefix := Bconcat(zetKeyPrefix, StringToBytesNoCopy(name), splitChar)
	realKey := Bconcat(keyPrefix, scoreStart, splitChar, keyStart)
	scoreBeginIndex := len(keyPrefix)
	scoreEndIndex := scoreBeginIndex + scoreByteLen
	keyBeginIndex := scoreBeginIndex + scoreByteLen + 1
	n := 0
	sliceRange := util.BytesPrefix(keyPrefix)
	if len(keyStart) == 0 {
		realKey = util.BytesPrefix(Bconcat(keyPrefix, scoreStart, splitChar)).Start
	}
	sliceRange.Limit = realKey
	iter := db.NewIterator(sliceRange, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		if bytes.Compare(realKey, iter.Key()) == 1 {
			r.Data = append(r.Data,
				append([]byte{}, iter.Key()[keyBeginIndex:]...),                // key
				append([]byte{}, iter.Key()[scoreBeginIndex:scoreEndIndex]...), // score
			)
			n++
			if n == limit {
				break
			}
		}
	}

	iter.Release()
	err := iter.Error()
	if err != nil {
		r.State = err.Error()
		r.Data = []BS{}
		return r
	}
	if n > 0 {
		r.State = replyOK
	}
	return r
}

func (r *Reply) OK() bool {
	return r.State == replyOK
}

func (r *Reply) NotFound() bool {
	return r.State == replyNotFound
}

func (r *Reply) Bytes() []byte {
	return r.bytex()
}

func (r *Reply) bytex() BS {
	if len(r.Data) > 0 {
		return r.Data[0]
	}
	return nil
}

// String is a convenience wrapper over Get for string value.
func (r *Reply) String() string {
	return r.bytex().String()
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (r *Reply) Int() int {
	return r.bytex().Int()
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (r *Reply) Int64() int64 {
	return r.bytex().Int64()
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (r *Reply) Uint() uint {
	return r.bytex().Uint()
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (r *Reply) Uint64() uint64 {
	return r.bytex().Uint64()
}

// List retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) List() []Entry {
	if len(r.Data) < 1 {
		return []Entry{}
	}
	list := make([]Entry, len(r.Data)/2)
	j := 0
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		list[j] = Entry{r.Data[i], r.Data[i+1]}
		j++
	}
	return list
}

// Dict retrieves the key/value pairs from reply of a hashmap.
func (r *Reply) Dict() map[string][]byte {
	if len(r.Data) < 1 {
		return map[string][]byte{}
	}
	dict := make(map[string][]byte, len(r.Data)/2)
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		dict[BytesToStringNoCopy(r.Data[i])] = r.Data[i+1]
	}
	return dict
}

func (r *Reply) KvLen() int {
	return len(r.Data) / 2
}

func (r *Reply) KvEach(fn func(key, value BS)) int {
	for i := 0; i < (len(r.Data) - 1); i += 2 {
		fn(r.Data[i], r.Data[i+1])
	}
	return r.KvLen()
}

func (b BS) Bytes() []byte {
	return b
}

func (b BS) String() string {
	return BytesToStringNoCopy(b)
}

// Int is a convenience wrapper over Get for int value of a hashmap.
func (b BS) Int() int {
	return int(b.Uint64())
}

// Int64 is a convenience wrapper over Get for int64 value of a hashmap.
func (b BS) Int64() int64 {
	return int64(b.Uint64())
}

// Uint is a convenience wrapper over Get for uint value of a hashmap.
func (b BS) Uint() uint {
	return uint(b.Uint64())
}

// Uint64 is a convenience wrapper over Get for uint64 value of a hashmap.
func (b BS) Uint64() uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// Bconcat concat a list of byte
func Bconcat(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

// DigitStringToBytes returns an 8-byte big endian representation of Digit string
// v ("123456") -> uint64(123456) -> 8-byte big endian.
func DigitStringToBytes(v string) []byte {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return []byte("")
	}
	return Uint64ToBytes(i)
}

// BytesToDigitString return a Digit string of v
// v (8-byte big endian) -> uint64(123456) -> "123456".
func BytesToDigitString(v []byte) string {
	return strconv.FormatUint(binary.BigEndian.Uint64(v), 10)
}

// DigitStringToUint64 returns uint64 of Digit string
// v ("123456") -> uint64(123456).
func DigitStringToUint64(v string) uint64 {
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return uint64(0)
	}
	return i
}

// Uint64ToBytes returns an 8-byte big endian representation of v
// v uint64(123456) -> 8-byte big endian.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// BytesToUint64 return an int64 of v
// v (8-byte big endian) -> uint64(123456).
func BytesToUint64(v []byte) uint64 {
	if len(v) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v[:8])
}

// BytesToStringNoCopy converts byte slice to a string without memory allocation.
// []byte("abc") -> "abc" s
func BytesToStringNoCopy(b []byte) string {
	/* #nosec G103 */
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytesNoCopy converts string to a byte slice without memory allocation.
// "abc" -> []byte("abc")
func StringToBytesNoCopy(s string) []byte {
	ptr := unsafe.StringData(s)
	b := unsafe.Slice(ptr, len(s))
	runtime.KeepAlive(&s)
	return b
}
