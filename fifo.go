package oura

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var fifoBucketName = []byte("oura-fifo")

type Fifo struct {
	path   string
	db     *bolt.DB
	head   uint64
	tail   uint64
	opened bool
	l      sync.RWMutex
}

func NewFifo(path string) (*Fifo, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	q := Fifo{
		path:   path,
		db:     db,
		head:   0,
		tail:   0,
		opened: false,
		l:      sync.RWMutex{},
	}
	return &q, q.init()
}

func (q *Fifo) init() error {
	q.l.Lock()
	defer q.l.Unlock()
	err := q.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(fifoBucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	q.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(fifoBucketName)

		c := b.Cursor()
		k, _ := c.First()
		if k != nil {
			q.head = keyToInt(k) - 1
		}
		k, _ = c.Last()
		if k != nil {
			q.tail = keyToInt(k)
		}
		return nil
	})
	q.opened = true
	return nil
}

func (q *Fifo) getByID(id uint64) ([]byte, error) {
	var val []byte
	if q.Empty() {
		return nil, ErrKeyNotFound
	}
	err := q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(fifoBucketName)
		v := b.Get(intToKey(id))
		if v == nil {
			return ErrKeyNotFound
		}
		val = append(val, v...)
		return nil
	})
	return val, err
}

func (q *Fifo) Peek() ([]byte, error) {
	if !q.opened {
		return nil, ErrDBClosed
	}
	q.l.RLock()
	defer q.l.RUnlock()
	return q.getByID(q.head + 1)
}

func (q *Fifo) Push(bts []byte) error {
	if !q.opened {
		return ErrDBClosed
	}
	q.l.Lock()
	defer q.l.Unlock()
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(fifoBucketName)
		err := b.Put(intToKey(q.tail+1), bts)
		return err
	})
	if err != nil {
		return err
	}
	q.tail++
	return nil
}

func (q *Fifo) Pop() ([]byte, error) {
	if !q.opened {
		return nil, ErrDBClosed
	}
	if q.Empty() {
		return nil, ErrEmptyQueue
	}
	q.l.Lock()
	defer q.l.Unlock()
	var bts []byte
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(fifoBucketName)
		v := b.Get(intToKey(q.head + 1))
		if v != nil {
			bts = append(bts, v...)
			err := b.Delete(intToKey(q.head + 1))
			if err != nil {
				return err
			}
			q.head++
			return nil
		}
		return ErrKeyNotFound
	})
	if err != nil {
		return nil, err
	}
	return bts, nil
}

func (q *Fifo) PushString(str string) error {
	return q.Push([]byte(str))
}

func (q *Fifo) PopString() (string, error) {
	b, err := q.Pop()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (q *Fifo) PushObject(obj interface{}) error {
	bts, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return q.Push(bts)
}

func (q *Fifo) PopObject(obj interface{}) error {
	bts, err := q.Pop()
	if err != nil {
		return err
	}
	return json.Unmarshal(bts, obj)
}

func (q *Fifo) PeekObject(obj interface{}) error {
	bts, err := q.Peek()
	if err != nil {
		return err
	}
	return json.Unmarshal(bts, obj)
}

func (q *Fifo) Length() uint64 {
	return q.tail - q.head
}

func (q *Fifo) Empty() bool {
	return q.Length() == uint64(0)
}

func (q *Fifo) Close() error {
	err := q.db.Close()
	if err != nil {
		return err
	}
	q.opened = false
	return nil
}
