package listobjects

import (
	"testing"
)

func TestListObjectsMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestListObjectsPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestListObjectsMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func testRunAll(t *testing.T, engine string) {
	t.Run("Schema1_1", func(t *testing.T) {
		RunSchema1_1ListObjectsTests(t, engine)
	})
	t.Run("Schema1_0", func(t *testing.T) {
		RunSchema1_0ListObjectsTests(t, engine)
	})
}
