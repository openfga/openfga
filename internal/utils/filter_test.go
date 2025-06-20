package utils

import (
	"reflect"
	"slices"
	"testing"
)

// TestFilterInts tests the Filter function with a slice of integers.
func TestFilterInts(t *testing.T) {
	tests := []struct {
		name      string
		input     []int
		predicate func(int) bool
		expected  []int
	}{
		{
			name:      "Filter even numbers",
			input:     []int{1, 2, 3, 4, 5, 6},
			predicate: func(n int) bool { return n%2 == 0 },
			expected:  []int{2, 4, 6},
		},
		{
			name:      "Filter numbers greater than 3",
			input:     []int{1, 2, 3, 4, 5},
			predicate: func(n int) bool { return n > 3 },
			expected:  []int{4, 5},
		},
		{
			name:      "Empty slice input",
			input:     []int{},
			predicate: func(n int) bool { return n > 0 },
			expected:  []int{},
		},
		{
			name:      "No elements satisfy predicate",
			input:     []int{1, 2, 3},
			predicate: func(n int) bool { return n > 10 },
			expected:  []int{},
		},
		{
			name:      "All elements satisfy predicate",
			input:     []int{1, 2, 3},
			predicate: func(n int) bool { return n > 0 },
			expected:  []int{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the Filter function to get an iterator sequence
			resultIter := Filter(tt.input, tt.predicate)
			// Collect all elements from the iterator into a slice
			got := slices.Collect(resultIter)

			if !slices.Equal(got, tt.expected) {
				t.Errorf("Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// person struct for testing with custom types.
type person struct {
	Name string
	Age  int
}

// TestFilterStructs tests the Filter function with a slice of custom structs.
func TestFilterStructs(t *testing.T) {
	people := []person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
		{"Diana", 25},
	}

	tests := []struct {
		name      string
		input     []person
		predicate func(person) bool
		expected  []person
	}{
		{
			name:      "Filter people older than 30",
			input:     people,
			predicate: func(p person) bool { return p.Age > 30 },
			expected:  []person{{"Charlie", 35}},
		},
		{
			name:      "Filter people with age 25",
			input:     people,
			predicate: func(p person) bool { return p.Age == 25 },
			expected:  []person{{"Bob", 25}, {"Diana", 25}},
		},
		{
			name:      "Filter people named Alice",
			input:     people,
			predicate: func(p person) bool { return p.Name == "Alice" },
			expected:  []person{{"Alice", 30}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultIter := Filter(tt.input, tt.predicate)
			got := slices.Collect(resultIter)

			// Use reflect.DeepEqual for comparing slices of structs
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("Filter() = %v, want %v", got, tt.expected)
			}
		})
	}
}
