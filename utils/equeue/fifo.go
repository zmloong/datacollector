package equeue

import (
	"container/list"
)

type Element struct {
	Key   string
	Value interface{}
}

type Cache_fifo struct {
	Len    int
	Equeue list.List
	Kmap   map[string]*list.Element
}

func (fifo *Cache_fifo) Init(len int) {
	fifo.Len = len
	fifo.Kmap = make(map[string]*list.Element)
}

func (fifo *Cache_fifo) Del(k string) error {
	e := fifo.Kmap[k]
	if e != nil {
		fifo.Equeue.Remove(e)
	}
	return nil
}

func (fifo *Cache_fifo) Put(ne Element) {
	if fifo.Equeue.Len() >= fifo.Len {
		ef := fifo.Equeue.Front()
		e := ef.Value.(Element)
		delete(fifo.Kmap, e.Key)
		fifo.Equeue.Remove(ef)
	}
	nep := fifo.Equeue.PushBack(ne)
	fifo.Kmap[ne.Key] = nep
}
