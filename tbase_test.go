package tbase

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

const dfmt = "2006-01-02"

func TestData(t *testing.T) {
	f, err := os.Open("fixtures/EBAY.csv")
	if err != nil {
		t.Error(err)
	}
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Error(err)
	}
	ts := NewTimeSeries("EBAY", rows[0][1:]...)
	for _, r := range rows[1:] {
		date, _ := time.Parse(dfmt, r[0])
		vals := []float64{}
		for _, v := range r[1:] {
			float, _ := strconv.ParseFloat(v, 64)
			vals = append(vals, float)
		}
		ts.AddObservation(date, vals)
	}
	fmt.Println(ts)
	db := TBase{NewBoltStorage("doot")}
	db.Persist(ts)
	// fmt.Println(db.EncodeRow("EBAY", time.Now(), []float64{1, 2, 3}))
}
