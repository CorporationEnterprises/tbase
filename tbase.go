package tbase

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

type Storage interface {
	Collection(string) Collection
}

type Collection interface {
	Encode([]byte, []byte) error
}

type BoltStorage struct {
	DB *bolt.DB
}

type TBase struct {
	storage Storage
}

type TimeSeries struct {
	Name        string
	ColumnNames []string
	Times       []time.Time
	Values      [][]float64
	Length      int
}

func NewTimeSeries(name string, cols ...string) *TimeSeries {
	return &TimeSeries{
		Name:        name,
		ColumnNames: cols,
	}
}

func NewBoltStorage(filepath string) *BoltStorage {
	db, err := bolt.Open(filepath, 0666, &bolt.Options{})
	if err != nil {
		panic(err)
	}
	return &BoltStorage{db}
}

type BoltCollection struct {
	b *bolt.Bucket
}

func (bs *BoltStorage) Collection(cname string) Collection {
	coll := BoltCollection{}
	bs.DB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(cname))
		if err != nil {
			panic(err)
		}
		coll.b = bucket
		return nil
	})
	return &coll
}

func (bc *BoltCollection) Encode(key, value []byte) error {
	return bc.b.Put(key, value)
}

func (t *TimeSeries) String() string {
	return fmt.Sprintf("%s(%s)", t.Name, strings.Join(t.ColumnNames, ", "))
}

func (t *TimeSeries) AddObservation(ts time.Time, data []float64) {
	if len(data) != len(t.ColumnNames) {
		panic("incorrect number of columns provided")
	}
	t.Times = append(t.Times, ts)
	t.Values = append(t.Values, data)
	t.Length++
}

func FloatSliceBytes(values []float64) []byte {
	bytes := []byte{}
	for _, f := range values {
		bytes = append(bytes, Float64bytes(f)...)
	}
	return bytes
}

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Float64bytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func (t *TBase) Persist(ts *TimeSeries) {
	coll := t.storage.Collection(ts.Name)
	for ix, r := range ts.Values {
		timeBytes, _ := ts.Times[ix].MarshalBinary()
		fbytes := FloatSliceBytes(r)
		coll.Encode(timeBytes, fbytes)
	}
}
