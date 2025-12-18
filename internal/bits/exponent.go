package bits

func PowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}
