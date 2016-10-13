package tbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

type Storage interface {
	CreateNamespace(name string) error
	Put(namespace string, key, val []byte) error
	BatchPut(namespace string, k, v [][]byte) error
	WithCursor(namespace string, f func(Cursor))
}

type CollectionMeta struct {
	Name    string
	Columns []string
}

type BoltStorage struct {
	DB *bolt.DB
}

// NewBoltStorage opens a bolt database at the given filepath and implements
// the interface allowing it to back a TBase
func NewBoltStorage(filepath string) *BoltStorage {
	db, err := bolt.Open(filepath, 0666, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	return &BoltStorage{db}
}

// CreateNamespace in bolt means creating a bucket
func (bs *BoltStorage) CreateNamespace(name string) error {
	var err error
	bs.DB.Update(func(tx *bolt.Tx) error {
		nameBytes := []byte(name)
		_, err = tx.CreateBucketIfNotExists(nameBytes)
		return err
	})
	return err
}

// BatchPut implementation for bolt
func (bs *BoltStorage) BatchPut(namespace string, k, v [][]byte) error {
	if len(k) != len(v) {
		return errors.New("keys and values must be the same length")
	}
	var err error
	bs.DB.Update(func(tx *bolt.Tx) error {
		nameBytes := []byte(namespace)
		buck := tx.Bucket(nameBytes)
		for ix := range k {
			err = buck.Put(k[ix], v[ix])
			if err != nil {
				break
			}
		}
		return err
	})
	return err
}

// Cursor represents a cursor on the data.
// We use the interface for bolt's cursor, because it represents how we want other
// backends to behave as well.
type Cursor interface {
	Seek(start []byte) (k, v []byte)
	First() (k, v []byte)
	Last() (k, v []byte)
	Prev() (k, v []byte)
	Next() (k, v []byte)
}

// WithCursor implementation for bolt
// Bolt does not have cursors that can live outside of a transaction, so we pass in a function
func (bs *BoltStorage) WithCursor(namespace string, f func(Cursor)) {
	nameBytes := []byte(namespace)
	bs.DB.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket(nameBytes)
		cur := buck.Cursor()
		f(cur)
		return nil
	})
}

// Put implementation for bolt
// Since the only way to put is in a transaction, just pass to BatchPut
func (bs *BoltStorage) Put(namespace string, k, v []byte) error {
	return bs.BatchPut(namespace, [][]byte{k}, [][]byte{v})
}

type TBase struct {
	storage Storage
}

func (t *TBase) ExecQuery(*Query) (*TimeSeries, error) {
	return &TimeSeries{}, nil
}

func (t *TBase) CreateCollection(name string, columns []string) error {
	err := t.storage.CreateNamespace("collections")
	if err != nil {
		return err
	}
	cbytes, _ := json.Marshal(CollectionMeta{name, columns})
	err = t.storage.Put("collections", []byte(name), cbytes)
	return err
}

func (t *TBase) Query() *Query {
	return &Query{
		db: t,
	}
}

// Persist turns a TimeSeries into a collection, and persists the values
func (t *TBase) Persist(ts *TimeSeries) error {
	err := t.CreateCollection(ts.Name, ts.ColumnNames)
	if err != nil {
		return err
	}
	keys := make([][]byte, len(ts.Values))
	vals := make([][]byte, len(ts.Values))
	for ix := range ts.Values {
		tbytes, err := ts.Times[ix].MarshalBinary()
		if err != nil {
			return err
		}
		keys = append(keys, tbytes)
		vals = append(vals, floatSliceToBytes(ts.Values[ix]))
	}
	return t.storage.BatchPut(ts.Name, keys, vals)
}

// Query represents instructions to load data from storage into memory.
// Methods return the underlying object to allow chaining syntax.
type Query struct {
	db         *TBase
	Collection string
	// Empty columns will return all columns
	SelectColumns []string
	// Zero times will assume min or max time, respectively
	Start time.Time
	End   time.Time
}

// Columns sets the column names to be returned for this query
func (q *Query) Columns(c ...string) *Query {
	q.SelectColumns = c
	return q
}

// Since sets the minimum time for the query
func (q *Query) Since(t time.Time) *Query {
	q.Start = t
	return q
}

// Until sets the maximum time for the query
func (q *Query) Until(t time.Time) *Query {
	q.End = t
	return q
}

// Exec triggers the underlying storage engine to perform the query
func (q *Query) Exec() (*TimeSeries, error) {
	return q.db.ExecQuery(q)
}

// TimeSeries represents a collection of float-format data over time.
type TimeSeries struct {
	Name        string
	ColumnNames []string
	Times       []time.Time
	Values      [][]float64
	Length      int
}

// NewTimeSeries creates an empty series with the given name and columns.
func NewTimeSeries(name string, cols []string) *TimeSeries {
	return &TimeSeries{
		Name:        name,
		ColumnNames: cols,
	}
}

func (t *TimeSeries) AddObservation(ts time.Time, data []float64) {
	if len(data) != len(t.ColumnNames) {
		panic("incorrect number of columns provided")
	}
	t.Times = append(t.Times, ts)
	t.Values = append(t.Values, data)
	t.Length++
}

func (t *TimeSeries) String() string {
	return fmt.Sprintf("%s(%s)", t.Name, strings.Join(t.ColumnNames, ", "))
}
