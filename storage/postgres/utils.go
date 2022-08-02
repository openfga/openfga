package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4"
	openfgaerrors "github.com/openfga/openfga/pkg/errors"
	log "github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type tupleRecord struct {
	store      string
	objectType string
	objectID   string
	relation   string
	user       string
	ulid       string
	insertedAt time.Time
}

func (t *tupleRecord) asTuple() *openfgapb.Tuple {
	return &openfgapb.Tuple{
		Key: &openfgapb.TupleKey{
			Object:   tupleUtils.BuildObject(t.objectType, t.objectID),
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
		return nil, storage.TupleIteratorDone
	}

	var record tupleRecord
	if err := t.rows.Scan(&record.store, &record.objectType, &record.objectID, &record.relation, &record.user, &record.ulid, &record.insertedAt); err != nil {
		return nil, handlePostgresError(err)
	}

	if t.rows.Err() != nil {
		return nil, handlePostgresError(t.rows.Err())
	}

	return &record, nil
}

// toArray converts the tupleIterator to an []*openfgapb.Tuple and a possibly empty continuation token. If the
// continuation token exists it is the ulid of the last element of the returned array.
func (t *tupleIterator) toArray(opts storage.PaginationOptions) ([]*openfgapb.Tuple, []byte, error) {
	defer t.Stop()

	var res []*openfgapb.Tuple
	var ulid string
	for i := 0; i < opts.PageSize; i++ {
		tupleRecord, err := t.next()
		if err != nil {
			if err == storage.TupleIteratorDone {
				return res, nil, nil
			}
			return nil, nil, err
		}
		ulid = tupleRecord.ulid
		res = append(res, tupleRecord.asTuple())
	}

	// Check if we are at the end of the iterator. If we are then we do not need to return a continuation token. This is
	// why we have LIMIT+1 in the query.
	if _, err := t.next(); errors.Is(err, storage.TupleIteratorDone) {
		return res, nil, nil
	}

	contToken, err := json.Marshal(newContToken(ulid, ""))
	if err != nil {
		return nil, nil, err
	}

	return res, contToken, nil
}

func (t *tupleIterator) Next() (*openfgapb.Tuple, error) {
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

func buildReadQuery(store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) (string, []any, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	builder := psql.Select("store", "object_type", "object_id", "relation", "_user", "ulid", "inserted_at").
		From("tuple").
		Where(squirrel.Eq{"store": store})
	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		builder = builder.Where(squirrel.Eq{"object_type": objectType})
	}
	if objectID != "" {
		builder = builder.Where(squirrel.Eq{"object_id": objectID})
	}
	if tupleKey.GetRelation() != "" {
		builder = builder.Where(squirrel.Eq{"relation": tupleKey.GetRelation()})
	}
	if tupleKey.GetUser() != "" {
		builder = builder.Where(squirrel.Eq{"_user": tupleKey.GetUser()})
	}
	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", nil, err
		}
		builder = builder.Where(squirrel.Gt{"ulid": token.Ulid})
	}
	builder = builder.OrderBy("ulid")
	if opts.PageSize != 0 {
		builder = builder.Limit(uint64(opts.PageSize + 1))
	}

	return builder.ToSql()
}

func buildReadUsersetTuplesQuery(store string, tupleKey *openfgapb.TupleKey) string {
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
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", err
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
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", err
		}
		if token.ObjectType != objectTypeFilter {
			return "", storage.ErrMismatchObjectType
		}
		stmt = fmt.Sprintf("%s AND ulid > '%s'", stmt, token.Ulid) // > here as we always return a continuation token
	}
	stmt = fmt.Sprintf("%s ORDER BY inserted_at ASC", stmt)
	if opts.PageSize > 0 {
		stmt = fmt.Sprintf("%s LIMIT %d", stmt, opts.PageSize) // + 1 is NOT used here as we always return a continuation token
	}
	return stmt, nil
}

func buildReadAuthorizationModelsQuery(store string, opts storage.PaginationOptions) (string, error) {
	stmt := fmt.Sprintf("SELECT DISTINCT authorization_model_id FROM authorization_model WHERE store = '%s'", store)
	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", err
		}
		stmt = fmt.Sprintf("%s AND authorization_model_id <= '%s'", stmt, token.Ulid)
	}
	stmt = fmt.Sprintf("%s ORDER BY authorization_model_id DESC LIMIT %d", stmt, opts.PageSize+1) // + 1 is used to determine whether to return a continuation token.
	return stmt, nil
}

func unmarshallContToken(from string) (*contToken, error) {
	var token contToken
	if err := json.Unmarshal([]byte(from), &token); err != nil {
		return nil, storage.ErrInvalidContinuationToken
	}
	return &token, nil
}

func rollbackTx(ctx context.Context, tx pgx.Tx, logger log.Logger) {
	if err := tx.Rollback(ctx); !errors.Is(err, pgx.ErrTxClosed) {
		logger.ErrorWithContext(ctx, "failed to rollback transaction", log.Error(err))
	}
}

func handlePostgresError(err error, args ...interface{}) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return openfgaerrors.ErrorWithStack(storage.ErrNotFound)
	} else if strings.Contains(err.Error(), "duplicate key value") {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return openfgaerrors.ErrorWithStack(storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE))
			}
		}
		return openfgaerrors.ErrorWithStack(storage.ErrCollision)
	}
	return openfgaerrors.ErrorWithStack(err)
}
