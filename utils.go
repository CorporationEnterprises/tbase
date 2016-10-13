package tbase

import (
	"encoding/binary"
	"math"
)

func floatSliceToBytes(values []float64) []byte {
	bytes := []byte{}
	for _, f := range values {
		bytes = append(bytes, floatToBytes(f)...)
	}
	return bytes
}

func floatSliceFromBytes(val []byte) []float64 {
	floats := []float64{}
	ctr := 0
	for ctr*8 < len(val) {
		ix := ctr * 8
		f := floatFromBytes(val[ix : ix+8])
		floats = append(floats, f)
		ctr++
	}
	return floats
}

func floatFromBytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func floatToBytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}
