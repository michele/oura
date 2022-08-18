package oura_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/michele/oura"
)

type BStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
}

func BenchmarkFifoBytes(b *testing.B) {
	os.Remove("./bench_fifo.db")
	q, _ := oura.NewFifo("./bench_fifo.db")
	b.Run(fmt.Sprintf("push_%d", b.N), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			q.Push([]byte("abc"))
		}
	})
	b.Run(fmt.Sprintf("pop_%d", b.N), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			q.Pop()
		}
	})
	obj := BStruct{
		A: 100,
		B: "A string",
	}
	b.Run(fmt.Sprintf("push_object_%d", b.N), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			q.PushObject(obj)
		}
	})
	b.Run(fmt.Sprintf("pop_object_%d", b.N), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			q.PopObject(&obj)
		}
	})
}
