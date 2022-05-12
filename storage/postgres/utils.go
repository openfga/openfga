package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4"
	openfgaerrors "github.com/openfga/openfga/pkg/errors"
	log "github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type tupleRecord struct {
	store      string
	objectType string
	objectId   string
	relation   string
	user       string
	ulid       string
	insertedAt time.Time
}

func (t *tupleRecord) asTuple() *openfga.Tuple {
	return &openfga.Tuple{
		Key: &openfga.TupleKey{
			Object:   tupleUtils.BuildObject(t.objectType, t.objectId),
			Relation: t.relation,
			User:     t.user,
		},
		Timestamp: timestamppb.New(t.insertedAt),
	}
}

type tupleIterator struct {
	rows pgx.Rows
}

func (t *tupleIterator) next() (*tupleRecord, error) {
	if !t.rows.Next() {
		t.Stop()
		return nil, iterator.Done
	}

	var record tupleRecord
	if err := t.rows.Scan(&record.store, &record.objectType, &record.objectId, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
		return nil, handlePostgresError(err)
	}

	if t.rows.Err() != nil {
		return nil, handlePostgresError(t.rows.Err())
	}

	return &record, nil
}

// toArray converts the tupleIterator to an []*openfga.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *tupleIterator) toArray(opts storage.PaginationOptions) ([]*openfga.Tuple, []byte, error) {
	defer t.Stop()
	var res []*openfga.Tuple
	var lastUlid string
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next()
		if err != nil {
			if err == iterator.Done {
				return res, nil, nil
			}
			return nil, nil, err
		}
		lastUlid = tupleRecord.ulid
		res = append(res, tupleRecord.asTuple())
	}

	// Check if we are at the end of the iterator. If we are then we do not need to return a continuation token. This is
	// why we have LIMIT+1 in the query.
	if _, err := t.next(); errors.Is(err, iterator.Done) {
		return res, nil, nil
	}

	return res, []byte(lastUlid), nil
}

func (t *tupleIterator) Next() (*openfga.Tuple, error) {
	record, err := t.next()
	if err != nil {
		return nil, err
	}
	return record.asTuple(), nil
}

func (t *tupleIterator) Stop() {
	t.rows.Close()
}

type contToken struct {
	Ulid       string `json:"ulid"`
	ObjectType string `json:"objectType"`
}

func newContToken(ulid, objectType string) *contToken {
	return &contToken{
		Ulid:       ulid,
		ObjectType: objectType,
	}
}

func buildReadQuery(store string, tupleKey *openfga.TupleKey, opts storage.PaginationOptions) string {
	stmt := fmt.Sprintf("SELECT store, object_type, object_id, relation, _user, ulid, inserted_at FROM tuple WHERE store = '%s'", store)
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		stmt = fmt.Sprintf("%s AND object_type = '%s'", stmt, objectType)
	}
	if objectID != "" {
		stmt = fmt.Sprintf("%s AND object_id = '%s'", stmt, objectID)
	}
	if tupleKey.GetRelation() != "" {
		stmt = fmt.Sprintf("%s AND relation = '%s'", stmt, tupleKey.GetRelation())
	}
	if tupleKey.GetUser() != "" {
		stmt = fmt.Sprintf("%s AND _user = '%s'", stmt, tupleKey.GetUser())
	}
	if opts.From != "" {
		stmt = fmt.Sprintf("%s AND ulid > '%s'", stmt, opts.From)
	}
	stmt = fmt.Sprintf("%s ORDER BY ulid", stmt)
	if opts.PageSize != 0 {
		stmt = fmt.Sprintf("%s LIMIT %d", stmt, opts.PageSize+1) // + 1 is used to determine whether to return a continuation token.
	}
	return stmt
}

func buildReadUsersetTuplesQuery(store string, tupleKey *openfga.TupleKey) string {
	stmt := fmt.Sprintf("SELECT store, object_type, object_id, relation, _user, ulid, inserted_at FROM tuple WHERE store = '%s' AND user_type = '%s'", store, tupleUtils.UserSet)
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		stmt = fmt.Sprintf("%s AND object_type = '%s'", stmt, objectType)
	}
	if objectID != "" {
		stmt = fmt.Sprintf("%s AND object_id = '%s'", stmt, objectID)
	}
	if tupleKey.GetRelation() != "" {
		stmt = fmt.Sprintf("%s AND relation = '%s'", stmt, tupleKey.GetRelation())
	}
	stmt = fmt.Sprintf("%s ORDER BY ulid", stmt)
	return stmt
}

func buildListStoresQuery(opts storage.PaginationOptions) (string, error) {
	stmt := "SELECT id, name, created_at, updated_at FROM store WHERE deleted_at IS NULL"
	if opts.From != "" {
		var token contToken
		if err := json.Unmarshal([]byte(opts.From), &token); err != nil {
			return "", storage.InvalidContinuationToken
		}
		stmt = fmt.Sprintf("%s AND id >= '%s'", stmt, token.Ulid)
	}
	stmt = fmt.Sprintf("%s ORDER BY id LIMIT %d", stmt, opts.PageSize+1)
	return stmt, nil
}

func buildReadChangesQuery(store, objectTypeFilter string, opts storage.PaginationOptions, horizonOffset time.Duration) (string, error) {
	stmt := fmt.Sprintf("SELECT ulid, object_type, object_id, relation, _user, operation, inserted_at FROM changelog WHERE store = '%s'", store)
	if objectTypeFilter != "" {
		stmt = fmt.Sprintf("%s AND object_type = '%s'", stmt, objectTypeFilter)
	}
	stmt = fmt.Sprintf("%s AND inserted_at < NOW() - interval '%dms'", stmt, horizonOffset.Milliseconds())
	if opts.From != "" {
		var token contToken
		if err := json.Unmarshal([]byte(opts.From), &token); err != nil {
			return "", storage.InvalidContinuationToken
		}
		if token.ObjectType != objectTypeFilter {
			return "", storage.MismatchObjectType
		}
		stmt = fmt.Sprintf("%s AND ulid > '%s'", stmt, token.Ulid) // > here as we always return a continuation token
	}
	stmt = fmt.Sprintf("%s ORDER BY inserted_at ASC", stmt)
	if opts.PageSize > 0 {
		stmt = fmt.Sprintf("%s LIMIT %d", stmt, opts.PageSize) // + 1 is NOT used here as we always return a continuation token
	}
	return stmt, nil
}

func buildReadAuthorizationModelsQuery(store string, opts storage.PaginationOptions) string {
	stmt := fmt.Sprintf("SELECT DISTINCT authorization_model_id FROM authorization_model WHERE store = '%s'", store)
	if opts.From != "" {
		stmt = fmt.Sprintf("%s AND authorization_model_id >= '%s'", stmt, opts.From)
	}
	stmt = fmt.Sprintf("%s ORDER BY authorization_model_id LIMIT %d", stmt, opts.PageSize+1) // + 1 is used to determine whether to return a continuation token.
	return stmt
}

func rollbackTx(ctx context.Context, tx pgx.Tx, logger log.Logger) {
	if err := tx.Rollback(ctx); !errors.Is(err, pgx.ErrTxClosed) {
		logger.ErrorWithContext(ctx, "failed to rollback transaction", log.Error(err))
	}
}

func handlePostgresError(err error, args ...interface{}) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return openfgaerrors.ErrorWithStack(storage.NotFound)
	} else if strings.Contains(err.Error(), "duplicate key value") {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfga.TupleKey); ok {
				return openfgaerrors.ErrorWithStack(storage.InvalidWriteInputError(tk, openfga.TupleOperation_WRITE))
			}
		}
	}
	return openfgaerrors.ErrorWithStack(err)
}
