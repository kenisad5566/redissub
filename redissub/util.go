package redissub

import "sort"

// Contains reports whether v is present in s.
func Contains(s []string, v string) bool {
	return Index(s, v) >= 0
}

// Index returns the index of the first occurrence of v in s, or -1 if
// not present.
func Index(s []string, v string) int {
	// "Contains" may be replaced with "Index(s, v) >= 0":
	for i, n := range s {
		if n == v {
			return i
		}
	}
	return -1
}

type Comparator func(a, b interface{}) int

func Sort(values []interface{}, comparator Comparator) {
	sort.Sort(sortable{values, comparator})
}

type sortable struct {
	values     []interface{}
	comparator Comparator
}

func (s sortable) Len() int {
	return len(s.values)
}
func (s sortable) Swap(i, j int) {
	s.values[i], s.values[j] = s.values[j], s.values[i]
}
func (s sortable) Less(i, j int) bool {
	return s.comparator(s.values[i], s.values[j]) < 0
}
