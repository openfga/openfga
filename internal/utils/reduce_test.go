package utils

import "testing"

// TestReduceInts tests the Reduce function with a slice of integers.
func TestReduceInts(t *testing.T) {
	tests := []struct {
		name        string
		input       []int
		initializer int
		reducer     func(int, int) int
		expected    int
	}{
		{
			name:        "Sum of numbers",
			input:       []int{1, 2, 3, 4, 5},
			initializer: 0,
			reducer:     func(acc int, val int) int { return acc + val },
			expected:    15,
		},
		{
			name:        "Product of numbers",
			input:       []int{1, 2, 3, 4, 5},
			initializer: 1,
			reducer:     func(acc int, val int) int { return acc * val },
			expected:    120,
		},
		{
			name:        "Find maximum number",
			input:       []int{5, 2, 9, 1, 7},
			initializer: 0,
			reducer: func(acc int, val int) int {
				if val > acc {
					return val
				}
				return acc
			},
			expected: 9,
		},
		{
			name:        "Single element",
			input:       []int{10},
			initializer: 0,
			reducer:     func(acc int, val int) int { return acc + val },
			expected:    10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Reduce(tt.input, tt.initializer, tt.reducer)
			if got != tt.expected {
				t.Errorf("Reduce() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestReduceStructs tests the Reduce function with custom structs.
func TestReduceStructs(t *testing.T) {
	people := []person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
	}

	tests := []struct {
		name        string
		input       []person
		initializer int
		reducer     func(int, person) int
		expected    int
	}{
		{
			name:        "Sum of ages",
			input:       people,
			initializer: 0,
			reducer:     func(acc int, p person) int { return acc + p.Age },
			expected:    90, // 30 + 25 + 35
		},
		{
			name:        "Count people older than 25",
			input:       people,
			initializer: 0,
			reducer: func(acc int, p person) int {
				if p.Age > 25 {
					return acc + 1
				}
				return acc
			},
			expected: 2, // Alice and Charlie
		},
		{
			name:        "Empty slice of people",
			input:       []person{},
			initializer: 0,
			reducer:     func(acc int, p person) int { return acc + p.Age },
			expected:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Reduce(tt.input, tt.initializer, tt.reducer)
			if got != tt.expected {
				t.Errorf("Reduce() = %v, want %v", got, tt.expected)
			}
		})
	}
}
