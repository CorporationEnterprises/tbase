package tbase

import (
	"math"
	"testing"
)

func encodeAndDecodeFloat(f float64, t *testing.T) {
	if floatFromBytes(floatToBytes(f)) != f {
		t.Errorf("Encoding failed for %f", f)
	}
}

func TestFloatEncoding(t *testing.T) {
	encodeAndDecodeFloat(1.4, t)
	encodeAndDecodeFloat(math.Pi, t)
	encodeAndDecodeFloat(1.77777733333, t)
}

func encodeAndDecodeFloatSlice(slice []float64, t *testing.T) {
	for ix, f := range floatSliceFromBytes(floatSliceToBytes(slice)) {
		if slice[ix] != f {
			t.Errorf("Got mismatched float: %f should match %f", slice[ix], f)
		}
	}
}

func TestFloatSliceEncoding(t *testing.T) {
	encodeAndDecodeFloatSlice([]float64{1, 2, 3, 4}, t)
	encodeAndDecodeFloatSlice([]float64{8.59685986, 5555, math.Pi}, t)
	encodeAndDecodeFloatSlice([]float64{1.11111111111111}, t)
}
