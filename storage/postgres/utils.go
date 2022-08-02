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
	sb := psql.Select("store", "object_type", "object_id", "relation", "_user", "ulid", "inserted_at").
		From("tuple").
		Where(squirrel.Eq{"store": store}).
		OrderBy("ulid")

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		sb = sb.Where(squirrel.Eq{"object_type": objectType})
	}
	if objectID != "" {
		sb = sb.Where(squirrel.Eq{"object_id": objectID})
	}
	if tupleKey.GetRelation() != "" {
		sb = sb.Where(squirrel.Eq{"relation": tupleKey.GetRelation()})
	}
	if tupleKey.GetUser() != "" {
		sb = sb.Where(squirrel.Eq{"_user": tupleKey.GetUser()})
	}
	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", nil, err
		}
		sb = sb.Where(squirrel.Gt{"ulid": token.Ulid})
	}
	if opts.PageSize != 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	return sb.ToSql()
}

func buildReadUsersetTuplesQuery(store string, tupleKey *openfgapb.TupleKey) (string, []any, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sb := psql.Select("store", "object_type", "object_id", "relation", "_user", "ulid", "inserted_at").
		From("tuple").
		Where(squirrel.Eq{"store": store}).
		Where(squirrel.Eq{"user_type": tupleUtils.UserSet}).
		OrderBy("ulid")

	objectType, objectID := tupleUtils.SplitObject(tupleKey.GetObject())
	if objectType != "" {
		sb = sb.Where(squirrel.Eq{"object_type": objectType})
	}
	if objectID != "" {
		sb = sb.Where(squirrel.Eq{"object_id": objectID})
	}
	if tupleKey.GetRelation() != "" {
		sb = sb.Where(squirrel.Eq{"relation": tupleKey.GetRelation()})
	}

	return sb.ToSql()
}

func buildListStoresQuery(opts storage.PaginationOptions) (string, []any, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sb := psql.Select("id", "name", "created_at", "updated_at").
		From("store").
		Where(squirrel.Eq{"deleted_at": nil}).
		OrderBy("id")

	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", nil, err
		}
		sb = sb.Where(squirrel.GtOrEq{"id": token.Ulid})
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	return sb.ToSql()
}

func buildReadChangesQuery(store, objectTypeFilter string, opts storage.PaginationOptions, horizonOffset time.Duration) (string, []any, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sb := psql.Select("ulid", "object_type", "object_id", "relation", "_user", "operation", "inserted_at").
		From("changelog").
		Where(squirrel.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at < NOW() - interval '%dms'", horizonOffset.Milliseconds())).
		OrderBy("inserted_at ASC")

	if objectTypeFilter != "" {
		sb = sb.Where(squirrel.Eq{"object_type": objectTypeFilter})
	}
	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", nil, err
		}
		if token.ObjectType != objectTypeFilter {
			return "", nil, storage.ErrMismatchObjectType
		}

		sb = sb.Where(squirrel.Gt{"ulid": token.Ulid}) // > as we always return a continuation token
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize)) // + 1 is NOT used here as we always return a continuation token
	}

	return sb.ToSql()
}

func buildReadAuthorizationModelsQuery(store string, opts storage.PaginationOptions) (string, []any, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sb := psql.Select("authorization_model_id").Distinct().
		From("authorization_model").
		Where(squirrel.Eq{"store": store}).
		OrderBy("authorization_model_id DESC")

	if opts.From != "" {
		token, err := unmarshallContToken(opts.From)
		if err != nil {
			return "", nil, err
		}
		sb = sb.Where(squirrel.LtOrEq{"authorization_model_id": token.Ulid})
	}
	if opts.PageSize > 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	return sb.ToSql()
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
