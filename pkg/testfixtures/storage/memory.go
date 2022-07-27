package storage

type memoryTestContainer struct{}

// NewMemoryTestContainer constructs an implementation of the DatastoreTestContainer interface
// for the in memory datastore engine.
func NewMemoryTestContainer() *memoryTestContainer {
	return &memoryTestContainer{}
}

func (m *memoryTestContainer) GetConnectionURI() string {
	return ""
}
