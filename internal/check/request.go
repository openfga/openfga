package check

import (
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/tuple"
)

type Request struct {
	*graph.ResolveCheckRequest // TODO: Once we finish migrating, move into this package and drop useless fields (VisitedPaths, RequestMetadata)
	cacheKey                   string
}

type RequestParams = graph.ResolveCheckRequestParams

func NewRequest(p RequestParams) (*Request, error) {
	req, err := graph.NewResolveCheckRequest(p)
	if err != nil {
		return nil, err
	}

	tup := tuple.From(req.GetTupleKey())
	cacheKeyString := tup.String() + req.GetInvariantCacheKey()
	hasher := xxhash.New()
	_, _ = hasher.WriteString(cacheKeyString)
	cacheKey := CacheKeyPrefix + strconv.FormatUint(hasher.Sum64(), 10)

	return &Request{
		ResolveCheckRequest: req,
		cacheKey:            cacheKey,
	}, nil
}

func (r *Request) GetCacheKey() string {
	return r.cacheKey
}
