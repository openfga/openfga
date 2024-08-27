package graph

// CloneResolveCheckResponse clones the provided ResolveCheckResponse.
//
// If 'r' defines a nil ResolutionMetadata then this function returns
// an empty value struct for the resolution metadata instead of nil.
func CloneResolveCheckResponse(r *ResolveCheckResponse) *ResolveCheckResponse {
	resolutionMetadata := &ResolveCheckResponseMetadata{
		DatastoreQueryCount: 0,
		CycleDetected:       false,
	}

	if r.GetResolutionMetadata() != nil {
		resolutionMetadata.DatastoreQueryCount = r.GetResolutionMetadata().DatastoreQueryCount
		resolutionMetadata.CycleDetected = r.GetResolutionMetadata().CycleDetected
	}

	return &ResolveCheckResponse{
		Allowed:            r.GetAllowed(),
		ResolutionMetadata: resolutionMetadata,
	}
}

type ResolveCheckResponse struct {
	Allowed            bool
	ResolutionMetadata *ResolveCheckResponseMetadata
}

func (r *ResolveCheckResponse) GetCycleDetected() bool {
	if r != nil {
		return r.GetResolutionMetadata().CycleDetected
	}

	return false
}

func (r *ResolveCheckResponse) GetAllowed() bool {
	if r != nil {
		return r.Allowed
	}

	return false
}

func (r *ResolveCheckResponse) GetResolutionMetadata() *ResolveCheckResponseMetadata {
	if r != nil {
		return r.ResolutionMetadata
	}

	return nil
}
