package typesystem

type SchemaVersion int

const (
	SchemaVersionUnspecified SchemaVersion = iota
	SchemaVersion1_0
	SchemaVersion1_1
)

func NewSchemaVersion(s string) SchemaVersion {
	switch s {
	case "", "1.0":
		return SchemaVersion1_0
	case "1.1":
		return SchemaVersion1_1
	default:
		return SchemaVersionUnspecified
	}
}

func (v SchemaVersion) String() string {
	switch v {
	case SchemaVersion1_0:
		return "1.0"
	case SchemaVersion1_1:
		return "1.1"
	default:
		return "unspecified"
	}
}
