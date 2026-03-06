package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openfga/openfga/pkg/server/commands"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
)

// ImportManager tracks active import goroutines for graceful shutdown.
type ImportManager struct {
	mu            sync.Mutex
	activeImports map[string]context.CancelFunc
	wg            sync.WaitGroup
	activeCount   atomic.Int32
	maxConcurrent int
}

// NewImportManager creates a new ImportManager.
func NewImportManager(maxConcurrent int) *ImportManager {
	return &ImportManager{
		activeImports: make(map[string]context.CancelFunc),
		maxConcurrent: maxConcurrent,
	}
}

// StartImport registers a new import and returns a cancellable context.
func (m *ImportManager) StartImport(parentCtx context.Context, importID string) (context.Context, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(m.activeCount.Load()) >= m.maxConcurrent {
		return nil, status.Errorf(codes.ResourceExhausted, "maximum concurrent imports (%d) reached", m.maxConcurrent)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	m.activeImports[importID] = cancel
	m.activeCount.Add(1)
	m.wg.Add(1)

	return ctx, nil
}

// FinishImport removes the import from tracking.
func (m *ImportManager) FinishImport(importID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.activeImports, importID)
	m.activeCount.Add(-1)
	m.wg.Done()
}

// Shutdown cancels all active imports and waits for them to finish.
func (m *ImportManager) Shutdown() {
	m.mu.Lock()
	for _, cancel := range m.activeImports {
		cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// CreateImportRequest represents a request to create an import job.
type CreateImportRequest struct {
	StoreID              string
	AuthorizationModelID string
	Source               string
	Format               string
}

// CreateImportResponse is the response from creating an import job.
type CreateImportResponse struct {
	ImportID string
	Status   string
}

// GetImportStatusRequest represents a request to get the status of an import.
type GetImportStatusRequest struct {
	StoreID  string
	ImportID string
}

// GetImportStatusResponse is the response containing import status and progress.
type GetImportStatusResponse struct {
	ImportID       string
	Status         string
	TuplesImported int64
	TuplesFailed   int64
	TuplesTotal    int64
	ErrorMessage   string
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

// CreateImport starts an async import job.
func (s *Server) CreateImport(ctx context.Context, req *CreateImportRequest) (*CreateImportResponse, error) {
	ctx, span := tracer.Start(ctx, "CreateImport", trace.WithAttributes(
		attribute.String("store_id", req.StoreID),
	))
	defer span.End()

	if !s.IsExperimentalEnabled(serverconfig.ExperimentalBulkImport) {
		return nil, status.Error(codes.Unimplemented, "bulk import is not enabled")
	}

	if req.Source == "" {
		return nil, status.Error(codes.InvalidArgument, "source is required")
	}
	if req.Format == "" {
		req.Format = "jsonl"
	}
	if req.Format != "jsonl" && req.Format != "json" {
		return nil, status.Error(codes.InvalidArgument, "format must be 'jsonl' or 'json'")
	}

	typesys, err := s.resolveTypesystem(ctx, req.StoreID, req.AuthorizationModelID)
	if err != nil {
		return nil, err
	}

	importID := ulid.Make().String()

	imp := &storage.Import{
		ID:      importID,
		Store:   req.StoreID,
		ModelID: typesys.GetAuthorizationModelID(),
		Source:  req.Source,
		Format:  req.Format,
		Status:  storage.ImportStatusPending,
	}

	if err := s.datastore.CreateImport(ctx, imp); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create import record: %v", err)
	}

	if s.importManager == nil {
		return nil, status.Error(codes.Internal, "import manager not initialized")
	}

	importCtx, err := s.importManager.StartImport(context.Background(), importID)
	if err != nil {
		_ = s.datastore.UpdateImportStatus(ctx, req.StoreID, importID, storage.ImportStatusFailed, err.Error())
		return nil, err
	}

	cmd := commands.NewImportDataCommand(
		s.datastore,
		typesys,
		req.StoreID,
		commands.WithImportLogger(s.logger),
	)

	go func() {
		defer s.importManager.FinishImport(importID)
		cmd.Run(importCtx, imp)
	}()

	return &CreateImportResponse{
		ImportID: importID,
		Status:   storage.ImportStatusPending,
	}, nil
}

// GetImportStatus returns the current status and progress of an import job.
func (s *Server) GetImportStatus(ctx context.Context, req *GetImportStatusRequest) (*GetImportStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "GetImportStatus", trace.WithAttributes(
		attribute.String("store_id", req.StoreID),
		attribute.String("import_id", req.ImportID),
	))
	defer span.End()

	if !s.IsExperimentalEnabled(serverconfig.ExperimentalBulkImport) {
		return nil, status.Error(codes.Unimplemented, "bulk import is not enabled")
	}

	if req.ImportID == "" {
		return nil, status.Error(codes.InvalidArgument, "import_id is required")
	}

	imp, err := s.datastore.GetImport(ctx, req.StoreID, req.ImportID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "import not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get import: %v", err)
	}

	return &GetImportStatusResponse{
		ImportID:       imp.ID,
		Status:         imp.Status,
		TuplesImported: imp.TuplesImported,
		TuplesFailed:   imp.TuplesFailed,
		TuplesTotal:    imp.TuplesTotal,
		ErrorMessage:   imp.ErrorMessage,
		CreatedAt:      imp.CreatedAt,
		CompletedAt:    imp.CompletedAt,
	}, nil
}

// IsExperimentalEnabled checks if an experimental feature flag is enabled.
func (s *Server) IsExperimentalEnabled(flag string) bool {
	for _, exp := range s.experimentals {
		if exp == flag {
			return true
		}
	}
	return false
}
