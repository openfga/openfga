package commands

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// ImportDataCommand processes a bulk import job in the background.
type ImportDataCommand struct {
	datastore                 storage.OpenFGADatastore
	typesys                   *typesystem.TypeSystem
	storeID                   string
	conditionContextByteLimit int
	logger                    logger.Logger
}

type ImportDataCommandOption func(*ImportDataCommand)

func WithImportLogger(l logger.Logger) ImportDataCommandOption {
	return func(cmd *ImportDataCommand) {
		cmd.logger = l
	}
}

func WithImportConditionContextByteLimit(limit int) ImportDataCommandOption {
	return func(cmd *ImportDataCommand) {
		cmd.conditionContextByteLimit = limit
	}
}

func NewImportDataCommand(
	datastore storage.OpenFGADatastore,
	typesys *typesystem.TypeSystem,
	storeID string,
	opts ...ImportDataCommandOption,
) *ImportDataCommand {
	cmd := &ImportDataCommand{
		datastore:                 datastore,
		typesys:                   typesys,
		storeID:                   storeID,
		conditionContextByteLimit: config.DefaultWriteContextByteLimit,
		logger:                    logger.NewNoopLogger(),
	}
	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

// Run processes the import. It should be called in a background goroutine.
func (c *ImportDataCommand) Run(ctx context.Context, imp *storage.Import) {
	// Update status to processing
	if err := c.datastore.UpdateImportStatus(ctx, c.storeID, imp.ID, storage.ImportStatusProcessing, ""); err != nil {
		c.logger.Error("failed to update import status to processing", zap.Error(err))
		return
	}

	reader, err := NewFileReader(imp.Source, imp.Format)
	if err != nil {
		c.failImport(ctx, imp.ID, fmt.Sprintf("failed to create file reader: %v", err))
		return
	}

	batchCh, errCh := reader.ReadBatches(ctx, config.DefaultImportDataBatchSize)

	var imported, failed, total int64
	batchCount := 0
	lastProgressUpdate := time.Now()

	for batch := range batchCh {
		if ctx.Err() != nil {
			c.interruptImport(ctx, imp.ID)
			return
		}

		validTuples, invalidCount := c.validateBatch(batch.Tuples)
		failed += int64(invalidCount)
		total += int64(len(batch.Tuples))

		if len(validTuples) > 0 {
			if err := c.datastore.BulkWrite(ctx, c.storeID, validTuples); err != nil {
				if ctx.Err() != nil {
					c.interruptImport(ctx, imp.ID)
					return
				}
				c.failImport(ctx, imp.ID, fmt.Sprintf("bulk write failed: %v", err))
				return
			}
			imported += int64(len(validTuples))
		}

		batchCount++
		if batchCount%10 == 0 || time.Since(lastProgressUpdate) >= 5*time.Second {
			_ = c.datastore.UpdateImportProgress(ctx, c.storeID, imp.ID, imported, failed, total)
			lastProgressUpdate = time.Now()
		}
	}

	// Check for reader errors
	select {
	case err := <-errCh:
		if err != nil {
			c.failImport(ctx, imp.ID, fmt.Sprintf("file read error: %v", err))
			return
		}
	default:
	}

	// Final progress update
	_ = c.datastore.UpdateImportProgress(ctx, c.storeID, imp.ID, imported, failed, total)

	// Mark completed
	if err := c.datastore.UpdateImportStatus(ctx, c.storeID, imp.ID, storage.ImportStatusCompleted, ""); err != nil {
		c.logger.Error("failed to update import status to completed", zap.Error(err))
	}
}

func (c *ImportDataCommand) validateBatch(tuples []*openfgav1.TupleKey) ([]*openfgav1.TupleKey, int) {
	var valid []*openfgav1.TupleKey
	invalidCount := 0

	for _, tk := range tuples {
		if err := validation.ValidateTupleForWrite(c.typesys, tk); err != nil {
			invalidCount++
			continue
		}

		if err := validateNotImplicit(tk); err != nil {
			invalidCount++
			continue
		}

		contextSize := proto.Size(tk.GetCondition().GetContext())
		if contextSize > c.conditionContextByteLimit {
			invalidCount++
			continue
		}

		valid = append(valid, tk)
	}

	return valid, invalidCount
}

func validateNotImplicit(tk *openfgav1.TupleKey) error {
	userObject, userRelation := tupleUtils.SplitObjectRelation(tk.GetUser())
	if tk.GetRelation() == userRelation && tk.GetObject() == userObject {
		return fmt.Errorf("cannot write a tuple that is implicit")
	}
	return nil
}

func (c *ImportDataCommand) failImport(ctx context.Context, importID, errMsg string) {
	c.logger.Error("import failed", zap.String("import_id", importID), zap.String("error", errMsg))
	if err := c.datastore.UpdateImportStatus(ctx, c.storeID, importID, storage.ImportStatusFailed, errMsg); err != nil {
		c.logger.Error("failed to update import status to failed", zap.Error(err))
	}
}

func (c *ImportDataCommand) interruptImport(_ context.Context, importID string) {
	c.logger.Info("import interrupted", zap.String("import_id", importID))
	// Use a new context since the original may be cancelled
	bgCtx := context.Background()
	if err := c.datastore.UpdateImportStatus(bgCtx, c.storeID, importID, storage.ImportStatusInterrupted, "server shutdown"); err != nil {
		c.logger.Error("failed to update import status to interrupted", zap.Error(err))
	}
}
