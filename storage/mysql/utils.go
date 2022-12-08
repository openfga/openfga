package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/go-sql-driver/mysql"
	log "github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	"github.com/pkg/errors"
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

func unmarshallContToken(from string) (*contToken, error) {
	var token contToken
	if err := json.Unmarshal([]byte(from), &token); err != nil {
		return nil, storage.ErrInvalidContinuationToken
	}
	return &token, nil
}

func buildReadQuery(store string, tupleKey *openfgapb.TupleKey, opts storage.PaginationOptions) (string, []any, error) {
	sb := squirrel.Select("store", "object_type", "object_id", "relation", "_user", "ulid", "inserted_at").
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
		sb = sb.Where(squirrel.GtOrEq{"ulid": token.Ulid})
	}
	if opts.PageSize != 0 {
		sb = sb.Limit(uint64(opts.PageSize + 1)) // + 1 is used to determine whether to return a continuation token.
	}

	return sb.ToSql()
}

func buildReadUsersetTuplesQuery(store string, tupleKey *openfgapb.TupleKey) (string, []any, error) {
	sb := squirrel.Select("store", "object_type", "object_id", "relation", "_user", "ulid", "inserted_at").
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
	sb := squirrel.Select("id", "name", "created_at", "updated_at").
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
	sb := squirrel.Select("ulid", "object_type", "object_id", "relation", "_user", "operation", "inserted_at").
		From("changelog").
		Where(squirrel.Eq{"store": store}).
		Where(fmt.Sprintf("inserted_at <= NOW() - INTERVAL %d MICROSECOND", horizonOffset.Microseconds())).
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
	sb := squirrel.Select("authorization_model_id").Distinct().
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

func rollbackTx(ctx context.Context, tx *sql.Tx, logger log.Logger) {
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		logger.ErrorWithContext(ctx, "failed to rollback transaction", log.Error(err))
	}
}

func handleMySQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	} else if errors.Is(err, storage.ErrIteratorDone) {
		return storage.ErrIteratorDone
	} else if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
		if len(args) > 0 {
			if tk, ok := args[0].(*openfgapb.TupleKey); ok {
				return storage.InvalidWriteInputError(tk, openfgapb.TupleOperation_TUPLE_OPERATION_WRITE)
			}
		}
		return storage.ErrCollision
	}
	return fmt.Errorf("mysql error: %w", err)
}
