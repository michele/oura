package oura_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/michele/oura"
)

type AStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
}

func TestFifoQueue(t *testing.T) {
	os.Remove("./test_fifo.db")
	q, err := oura.NewFifo("./test_fifo.db")
	assert.NoError(t, err, "Creating a db shouldn't return an error")

	assert.Equal(t, uint64(0), q.Length())

	_, err = q.Peek()
	assert.ErrorIs(t, err, oura.ErrEmptyQueue)
	_, err = q.Pop()
	assert.ErrorIs(t, err, oura.ErrEmptyQueue)
	bts, err := q.Pop()
	assert.Error(t, err)
	assert.Nil(t, bts)

	err = q.Push([]byte("1"))
	assert.NoError(t, err)

	bts, err = q.Peek()
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), bts)

	assert.Equal(t, uint64(1), q.Length())
	bts, err = q.Pop()
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), bts)

	_, err = q.Pop()
	assert.Equal(t, oura.ErrEmptyQueue, err)

	err = q.PushString("a")
	assert.NoError(t, err)

	str, err := q.PopString()
	assert.NoError(t, err)
	assert.Equal(t, "a", str)

	obj := AStruct{
		A: 1,
		B: "test",
	}
	err = q.PushObject(obj)
	assert.NoError(t, err)

	var parsedObj AStruct
	err = q.PopObject(&parsedObj)
	assert.NoError(t, err)
	assert.ObjectsAreEqual(obj, parsedObj)

	err = q.PushString("a")
	assert.NoError(t, err)
	err = q.PushString("b")
	assert.NoError(t, err)
	err = q.PushString("c")
	assert.NoError(t, err)
	err = q.PushString("d")
	assert.NoError(t, err)
	str, err = q.PopString()
	assert.NoError(t, err)
	assert.Equal(t, "a", str)
	str, err = q.PopString()
	assert.NoError(t, err)
	assert.Equal(t, "b", str)
	str, err = q.PopString()
	assert.NoError(t, err)
	assert.Equal(t, "c", str)
	str, err = q.PopString()
	assert.NoError(t, err)
	assert.Equal(t, "d", str)

	err = q.Close()
	assert.NoError(t, err)
}
