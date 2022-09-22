package graph

import (
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/go-errors/errors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// RelationshipIngressType is used to define an enum of the type of ingresses between
// source object references and target user references thatexist in the graph of
// relationships.
type RelationshipIngressType int

const (
	// DirectIngress defines a direct ingress connection between a source object reference
	// and some target user reference.
	DirectIngress RelationshipIngressType = iota
	TupleToUsersetIngress
)

// RelationshipIngress represents a possible ingress point between some source object reference
// and a target user reference.
type RelationshipIngress struct {

	// The type of the relationship ingress
	Type RelationshipIngressType

	// The relationship reference that defines the ingress to some target relation
	Ingress *openfgapb.RelationReference
}

// ConnectedObjectGraph represents a graph of relationships and the connectivity between
// object and relation references within the graph through direct or indirect relationships.
// The ConnectedObjectGraph should be used to introspect what kind of relationships between
// object types can exist.
type ConnectedObjectGraph struct{}

// BuildConnectedObjectGraph builds an object graph representing the graph of relationships between connected
// object types either through direct or indirect relationships.
func BuildConnectedObjectGraph(t *typesystem.TypeSystem) *ConnectedObjectGraph {

	return &ConnectedObjectGraph{
		// ... todo: fill out necessary fields
	}
}

// RelationshipIngresses computes the incoming edges (ingresses) that are possible
// between the source object and relation and the target user (user or userset).
func (c *ConnectedObjectGraph) RelationshipIngresses(
	targetUserRef *openfgapb.RelationReference,
	sourceObjectRef *openfgapb.RelationReference,
) ([]*RelationshipIngress, error) {
	return nil, errors.New("unimplemented")
}
