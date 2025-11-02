package storage

type Set[T comparable] struct {
	set map[T]struct{}
	length int
}

func NewSet[T comparable]() *Set[T] {
	newSet := &Set[T]{
		set: make(map[T]struct{}), 
		length: 0,
	}
	return newSet
}

func (s *Set[T]) Add(entry T) {
	if _, ok := s.set[entry]; !ok {
		s.set[entry] = struct{}{}
		s.length++
	}
}

func (s *Set[T]) Items() map[T]struct{} {
	return s.set
}

func (s *Set[T]) Remove(entry T) {
	if _, ok := s.set[entry]; ok {
		delete(s.set, entry)
		s.length--
	}
}

func (s *Set[T]) Has(entry T) bool {
	_, ok := s.set[entry]
	return ok
}

func (s *Set[T]) Length() int {
	return s.length
}