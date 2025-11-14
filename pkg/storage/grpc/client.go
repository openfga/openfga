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
	// Addr is the address of the gRPC storage server (e.g., "localhost:50051").
	Addr string

	// TLSConfig is the TLS configuration. If nil, insecure credentials are used.
	TLSConfig *tls.Config

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

	if config.TLSConfig != nil {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewTLS(config.TLSConfig)))
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
		return storage.ReadinessStatus{}, fmt.Errorf("grpc is ready failed: %w", err)
	}

	return storage.ReadinessStatus{
		Message: resp.Message,
		IsReady: resp.IsReady,
	}, nil
}

func (c *Client) Read(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
	return nil, nil
}

func (c *Client) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	return nil, "", nil
}

func (c *Client) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	return nil, nil
}

func (c *Client) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	return nil, nil
}

func (c *Client) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	return nil, nil
}

func (c *Client) Write(ctx context.Context, store string, d storage.Deletes, w storage.Writes, opts ...storage.TupleWriteOption) error {
	return nil
}

func (c *Client) MaxTuplesPerWrite() int {
	return c.maxTuplesPerWriteField
}

func (c *Client) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	return nil, nil
}

func (c *Client) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	return nil, "", nil
}

func (c *Client) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	return nil, nil
}

func (c *Client) MaxTypesPerAuthorizationModel() int {
	return c.maxTypesPerModelField
}

func (c *Client) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	return nil
}

func (c *Client) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	return nil, nil
}

func (c *Client) DeleteStore(ctx context.Context, id string) error {
	return nil
}

func (c *Client) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	return nil, nil
}

func (c *Client) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	return nil, "", nil
}

func (c *Client) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	return nil
}

func (c *Client) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	return nil, nil
}

func (c *Client) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	return nil, "", nil
}
