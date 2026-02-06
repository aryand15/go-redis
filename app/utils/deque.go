package utils

import "fmt"

type Deque[T comparable] struct {
	head *Node[T]
	tail *Node[T]
	size int
}

type Node[T comparable] struct {
	val T
	next *Node[T]
	prev *Node[T]
}

func NewDeque[T comparable]() *Deque[T] {
	return &Deque[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (d *Deque[T]) IsEmpty() bool {
	return d.size == 0
}

func (d *Deque[T]) Clear() {
	d.head = nil
	d.tail = nil
	d.size = 0
}

func (d *Deque[T]) PeekLeft() (T, error) {
	if d.IsEmpty() {
		var zero T
		return zero, fmt.Errorf("cannot peek on empty deque")
	}

	return d.head.val, nil
}

func (d *Deque[T]) PeekRight() (T, error) {
	if d.IsEmpty() {
		var zero T
		return zero, fmt.Errorf("cannot peek on empty deque")
	}

	return d.tail.val, nil
}

func (d *Deque[T]) DeleteAt(idx int) error {
	switch {
	case idx < 0 || idx >= d.size:
		return fmt.Errorf("cannot delete at index %d for a deque of size %d", idx, d.size)
	case idx == 0:
		_, err := d.PopLeft()
		return err
	case idx == d.size - 1:
		_, err := d.PopRight()
		return err
	default:
		curr := d.head
		for i := 0; i < idx; i++ {
			curr = curr.next
		}
		curr.prev.next = curr.next
		curr.next.prev = curr.prev
		return nil
	}
}

func (d *Deque[T]) DeleteVal(val T) error {
	curr := d.head
	i := 0
	for ; i < d.size && curr.val != val; i++ {
		curr = curr.next
	}
	if i == d.size {
		return fmt.Errorf("element with given value was not found in deque")
	}
	if i == 0 {
		_, err := d.PopLeft()
		return err
	} else if i == d.size - 1 {
		_, err := d.PopRight()
		return err
	}

	curr.prev.next = curr.next
	curr.next.prev = curr.prev
	return nil
}

func (d *Deque[T]) PushRight(elem T) {
	elemNode := &Node[T]{
		val: elem,
		next: nil,
		prev: d.tail,
	}

	// Handle empty deque
	if d.IsEmpty() {
		d.head = elemNode
		d.tail = elemNode
	
	// Otherwise add element and update tail pointer
	} else {
		d.tail.next = elemNode
		d.tail = elemNode
	}

	d.size++
	
}

func (d *Deque[T]) PushLeft(elem T) {
	elemNode := &Node[T]{
		val: elem,
		next: d.head,
		prev: nil,
	}

	// Handle empty deque
	if d.IsEmpty() {
		d.head = elemNode
		d.tail = elemNode
	
	// Otherwise add element and update head pointer
	} else {
		d.head.prev = elemNode
		d.head = elemNode
	}

	d.size++
}

func (d *Deque[T]) PopRight() (T, error) {
	// Handle empty deque
	if d.IsEmpty() {
		var zero T
		return zero, fmt.Errorf("cannot pop empty deque")
	}

	popped := d.tail.val

	// Handle one-element deque
	if d.head == d.tail {
		d.head = nil
		d.tail = nil

	// Otherwise pop element from right and update pointers
	} else {
		d.tail = d.tail.prev
		d.tail.next = nil
	}

	d.size--
	return popped, nil
}

func (d *Deque[T]) PopLeft() (T, error) {
	// Handle empty deque
	if d.IsEmpty() {
		var zero T
		return zero, fmt.Errorf("cannot pop empty deque")
	}

	popped := d.head.val

	// Handle one-element deque
	if d.head == d.tail {
		d.head = nil
		d.tail = nil

	// Otherwise pop element from right and update pointers
	} else {
		d.head = d.head.next
		d.head.prev = nil
	}

	d.size--
	return popped, nil
}

func (d *Deque[T]) Range(start int, stop int) (res []T, err error) {
	if start < 0 || stop >= d.size {
		return nil, fmt.Errorf("Invalid range: [%d, %d] for deque of size %d", start, stop, d.size)
	}

	res = make([]T, stop-start+1)
	curr := d.head

	for i := 0; i <= stop; i++ {
		if i >= start {
			res[i] = curr.val
		}
		curr = curr.next
	}

	return res, nil

}

func (d Deque[T]) ToArray() []T {
	res, _ := d.Range(0, d.size)
	return res
}


