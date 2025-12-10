package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

// Client implements storage.OpenFGADatastore by making gRPC calls to a remote storage service.
type Client struct {
	conn                   *grpc.ClientConn
	client                 storagev1.StorageServiceClient
	maxTuplesPerWriteField int
	maxTypesPerModelField  int
}

var _ storage.OpenFGADatastore = (*Client)(nil)

// ClientConfig configures the gRPC storage client.
type ClientConfig struct {
	// Addr is the address of the gRPC storage server.
	// For TCP connections, use "host:port" format (e.g., "localhost:50051").
	// For Unix domain sockets, use "unix:///path/to/socket" format (e.g., "unix:///var/run/openfga.sock").
	Addr string

	// TLSCertPath is the path to the TLS certificate file.
	// If provided, TLSKeyPath must also be provided to enable TLS.
	TLSCertPath string

	// TLSKeyPath is the path to the TLS key file.
	// If provided, TLSCertPath must also be provided to enable TLS.
	TLSKeyPath string

	// KeepaliveTime is the duration after which a keepalive ping is sent if no activity.
	// Zero value means keepalive is disabled.
	KeepaliveTime time.Duration

	// KeepaliveTimeout is the duration to wait for a keepalive ping response.
	// Only used when KeepaliveTime > 0.
	KeepaliveTimeout time.Duration

	// KeepalivePermitWithoutStream allows sending keepalive pings even when no streams are active.
	// Only used when KeepaliveTime > 0.
	KeepalivePermitWithoutStream bool

	// MaxTuplesPerWrite is the maximum number of tuples that can be written in a single write operation.
	// If zero, defaults to storage.DefaultMaxTuplesPerWrite.
	MaxTuplesPerWrite int

	// MaxTypesPerAuthorizationModel is the maximum number of types allowed per authorization model.
	// If zero, defaults to storage.DefaultMaxTypesPerAuthorizationModel.
	MaxTypesPerAuthorizationModel int
}

func NewClient(config ClientConfig) (*Client, error) {
	grpcOpts := []grpc.DialOption{}

	if config.KeepaliveTime > 0 {
		grpcOpts = append(grpcOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                config.KeepaliveTime,
			Timeout:             config.KeepaliveTimeout,
			PermitWithoutStream: config.KeepalivePermitWithoutStream,
		}))
	}

	// Configure TLS if cert or key path is provided
	hasCert := config.TLSCertPath != ""
	hasKey := config.TLSKeyPath != ""

	if hasCert || hasKey {
		// If either cert or key is provided, both must be provided
		if !hasCert || !hasKey {
			return nil, fmt.Errorf("both TLS certificate and key paths must be provided together (cert: %q, key: %q)", config.TLSCertPath, config.TLSKeyPath)
		}

		cert, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}

		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(config.Addr, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC storage server: %w", err)
	}

	maxTuplesPerWrite := config.MaxTuplesPerWrite
	if maxTuplesPerWrite == 0 {
		maxTuplesPerWrite = storage.DefaultMaxTuplesPerWrite
	}

	maxTypesPerModel := config.MaxTypesPerAuthorizationModel
	if maxTypesPerModel == 0 {
		maxTypesPerModel = storage.DefaultMaxTypesPerAuthorizationModel
	}

	return &Client{
		conn:                   conn,
		client:                 storagev1.NewStorageServiceClient(conn),
		maxTuplesPerWriteField: maxTuplesPerWrite,
		maxTypesPerModelField:  maxTypesPerModel,
	}, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Client) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	req := &storagev1.IsReadyRequest{}

	resp, err := c.client.IsReady(ctx, req)
	if err != nil {
		return storage.ReadinessStatus{}, fromGRPCError(err)
	}

	return storage.ReadinessStatus{
		Message: resp.GetMessage(),
		IsReady: resp.GetIsReady(),
	}, nil
}

func (c *Client) Read(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
	req := &storagev1.ReadRequest{
		Store: store,
		Filter: &storagev1.ReadFilter{
			Object:     filter.Object,
			Relation:   filter.Relation,
			User:       filter.User,
			Conditions: filter.Conditions,
		},
		Consistency: &storagev1.ConsistencyOptions{
			Preference: storagev1.ConsistencyPreference(options.Consistency.Preference),
		},
	}

	stream, err := c.client.Read(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	return newStreamTupleIterator(stream), nil
}

func (c *Client) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	req := &storagev1.ReadPageRequest{
		Store: store,
		Filter: &storagev1.ReadFilter{
			Object:     filter.Object,
			Relation:   filter.Relation,
			User:       filter.User,
			Conditions: filter.Conditions,
		},
		Pagination: &storagev1.PaginationOptions{
			PageSize: int32(options.Pagination.PageSize),
			From:     options.Pagination.From,
		},
		Consistency: &storagev1.ConsistencyOptions{
			Preference: storagev1.ConsistencyPreference(options.Consistency.Preference),
		},
	}

	resp, err := c.client.ReadPage(ctx, req)
	if err != nil {
		return nil, "", fromGRPCError(err)
	}

	return fromStorageTuples(resp.GetTuples()), resp.GetContinuationToken(), nil
}

func (c *Client) ReadUserTuple(ctx context.Context, store string, filter storage.ReadUserTupleFilter, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	req := &storagev1.ReadUserTupleRequest{
		Store: store,
		Filter: &storagev1.ReadFilter{
			Object:     filter.Object,
			Relation:   filter.Relation,
			User:       filter.User,
			Conditions: filter.Conditions,
		},
		Consistency: &storagev1.ConsistencyOptions{
			Preference: storagev1.ConsistencyPreference(options.Consistency.Preference),
		},
	}

	resp, err := c.client.ReadUserTuple(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	// Guard against improper server implementations that return nil tuple
	if resp.GetTuple() == nil {
		return nil, storage.ErrNotFound
	}

	return fromStorageTuple(resp.GetTuple()), nil
}

func (c *Client) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	req := &storagev1.ReadUsersetTuplesRequest{
		Store: store,
		Filter: &storagev1.ReadUsersetTuplesFilter{
			Object:                      filter.Object,
			Relation:                    filter.Relation,
			AllowedUserTypeRestrictions: toStorageRelationReferences(filter.AllowedUserTypeRestrictions),
			Conditions:                  filter.Conditions,
		},
		Consistency: &storagev1.ConsistencyOptions{
			Preference: storagev1.ConsistencyPreference(options.Consistency.Preference),
		},
	}

	stream, err := c.client.ReadUsersetTuples(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	return newStreamTupleIterator(stream), nil
}

func (c *Client) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	var objectIDs []string = nil
	if filter.ObjectIDs != nil {
		objectIDs = filter.ObjectIDs.Values()
	}

	req := &storagev1.ReadStartingWithUserRequest{
		Store: store,
		Filter: &storagev1.ReadStartingWithUserFilter{
			ObjectType: filter.ObjectType,
			Relation:   filter.Relation,
			UserFilter: toStorageObjectRelations(filter.UserFilter),
			ObjectIds:  objectIDs,
			Conditions: filter.Conditions,
		},
		Consistency: &storagev1.ConsistencyOptions{
			Preference: storagev1.ConsistencyPreference(options.Consistency.Preference),
		},
		WithResultsSortedAscending: options.WithResultsSortedAscending,
	}

	stream, err := c.client.ReadStartingWithUser(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	return newStreamTupleIterator(stream), nil
}

func (c *Client) Write(ctx context.Context, store string, d storage.Deletes, w storage.Writes, opts ...storage.TupleWriteOption) error {
	writeOpts := storage.NewTupleWriteOptions(opts...)

	req := &storagev1.WriteRequest{
		Store:   store,
		Deletes: toStorageTupleKeysFromDeletes(d),
		Writes:  toStorageTupleKeys(w),
		Options: &storagev1.TupleWriteOptions{
			OnMissingDelete:   storagev1.OnMissingDelete(writeOpts.OnMissingDelete),
			OnDuplicateInsert: storagev1.OnDuplicateInsert(writeOpts.OnDuplicateInsert),
		},
	}

	_, err := c.client.Write(ctx, req)
	if err != nil {
		return fromGRPCError(err)
	}

	return nil
}

func (c *Client) MaxTuplesPerWrite() int {
	return c.maxTuplesPerWriteField
}

func (c *Client) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	req := &storagev1.ReadAuthorizationModelRequest{
		Store: store,
		Id:    id,
	}

	resp, err := c.client.ReadAuthorizationModel(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	model := fromStorageAuthorizationModel(resp.GetModel())

	// Guard against improper server implementations that return nil model or model with zero types.
	// Per the storage interface contract: "If it's not found, or if the model has zero types, it must return ErrNotFound."
	if model == nil || len(model.GetTypeDefinitions()) == 0 {
		return nil, storage.ErrNotFound
	}

	return model, nil
}

func (c *Client) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	req := &storagev1.ReadAuthorizationModelsRequest{
		Store: store,
		Pagination: &storagev1.PaginationOptions{
			PageSize: int32(options.Pagination.PageSize),
			From:     options.Pagination.From,
		},
	}

	resp, err := c.client.ReadAuthorizationModels(ctx, req)
	if err != nil {
		return nil, "", fromGRPCError(err)
	}

	return fromStorageAuthorizationModels(resp.GetModels()), resp.GetContinuationToken(), nil
}

func (c *Client) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	req := &storagev1.FindLatestAuthorizationModelRequest{
		Store: store,
	}

	resp, err := c.client.FindLatestAuthorizationModel(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	model := fromStorageAuthorizationModel(resp.GetModel())

	// Guard against improper server implementations that return nil model or model with zero types.
	// Per the storage interface contract: "If none were ever written, it must return ErrNotFound."
	if model == nil || len(model.GetTypeDefinitions()) == 0 {
		return nil, storage.ErrNotFound
	}

	return model, nil
}

func (c *Client) MaxTypesPerAuthorizationModel() int {
	return c.maxTypesPerModelField
}

func (c *Client) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	req := &storagev1.WriteAuthorizationModelRequest{
		Store: store,
		Model: toStorageAuthorizationModel(model),
	}

	_, err := c.client.WriteAuthorizationModel(ctx, req)
	if err != nil {
		return fromGRPCError(err)
	}

	return nil
}

func (c *Client) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	req := &storagev1.CreateStoreRequest{
		Store: toStorageStore(store),
	}

	resp, err := c.client.CreateStore(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	return fromStorageStore(resp.GetStore()), nil
}

func (c *Client) DeleteStore(ctx context.Context, id string) error {
	req := &storagev1.DeleteStoreRequest{
		Id: id,
	}

	_, err := c.client.DeleteStore(ctx, req)
	if err != nil {
		return fromGRPCError(err)
	}

	return nil
}

func (c *Client) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	req := &storagev1.GetStoreRequest{
		Id: id,
	}

	resp, err := c.client.GetStore(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	store := fromStorageStore(resp.GetStore())

	// Guard against improper server implementations that return nil store or a deleted store
	if store == nil || store.GetDeletedAt() != nil {
		return nil, storage.ErrNotFound
	}

	return store, nil
}

func (c *Client) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	req := &storagev1.ListStoresRequest{
		Ids:  options.IDs,
		Name: options.Name,
		Pagination: &storagev1.PaginationOptions{
			PageSize: int32(options.Pagination.PageSize),
			From:     options.Pagination.From,
		},
	}

	resp, err := c.client.ListStores(ctx, req)
	if err != nil {
		return nil, "", fromGRPCError(err)
	}

	return fromStorageStores(resp.GetStores()), resp.GetContinuationToken(), nil
}

func (c *Client) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	req := &storagev1.WriteAssertionsRequest{
		Store:      store,
		ModelId:    modelID,
		Assertions: toStorageAssertions(assertions),
	}

	_, err := c.client.WriteAssertions(ctx, req)
	if err != nil {
		return fromGRPCError(err)
	}

	return nil
}

func (c *Client) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	req := &storagev1.ReadAssertionsRequest{
		Store:   store,
		ModelId: modelID,
	}

	resp, err := c.client.ReadAssertions(ctx, req)
	if err != nil {
		return nil, fromGRPCError(err)
	}

	assertions := fromStorageAssertions(resp.GetAssertions())

	// Guard against improper server implementations that return nil assertions.
	// Per the contract: "If no assertions were ever written, it must return an empty list."
	if assertions == nil {
		assertions = []*openfgav1.Assertion{}
	}

	return assertions, nil
}

func (c *Client) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	req := &storagev1.ReadChangesRequest{
		Store: store,
		Filter: &storagev1.ReadChangesFilter{
			ObjectType:      filter.ObjectType,
			HorizonOffsetMs: filter.HorizonOffset.Milliseconds(),
		},
		Pagination: &storagev1.PaginationOptions{
			PageSize: int32(options.Pagination.PageSize),
			From:     options.Pagination.From,
		},
		SortDesc: options.SortDesc,
	}

	resp, err := c.client.ReadChanges(ctx, req)
	if err != nil {
		return nil, "", fromGRPCError(err)
	}

	changes := fromStorageTupleChanges(resp.GetChanges())

	// Guard against improper server implementations.
	// Per the contract: "if no changes are found, it should return storage.ErrNotFound and an empty continuation token."
	if len(changes) == 0 {
		return nil, "", storage.ErrNotFound
	}

	return changes, resp.GetContinuationToken(), nil
}
