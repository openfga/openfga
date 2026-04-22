package pipeline_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/listobjects/pipeline"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestReaderRead(t *testing.T) {
	t.Run("yields objects from matching tuples", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		tuples := []*openfgav1.Tuple{
			{Key: &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}},
		}

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		var got []string
		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			item, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
			if item.Err != nil {
				t.Fatalf("unexpected error: %v", item.Err)
			}
			got = append(got, item.Value)
		}

		want := []string{"document:1", "document:2"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("yields error when storage fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		sentinel := errors.New("connection refused")
		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(nil, sentinel)

		reader := pipeline.NewValidatingStore(store, "store-1")

		var gotErr error
		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			item, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
			gotErr = item.Err
		}

		if !errors.Is(gotErr, sentinel) {
			t.Fatalf("got error %v, want %v", gotErr, sentinel)
		}
	})

	t.Run("empty result set yields nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(storage.NewStaticTupleIterator(nil), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		count := 0
		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			item, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
			if item.Err != nil {
				t.Fatalf("unexpected error: %v", item.Err)
			}
			count++
		}

		if count != 0 {
			t.Fatalf("expected no items, got %d", count)
		}
	})

	t.Run("validator filters out invalid tuples", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		tuples := []*openfgav1.Tuple{
			{Key: &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:3", Relation: "viewer", User: "user:alice"}},
		}

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		// Only allow odd-numbered documents.
		validator := func(tk *openfgav1.TupleKey) (bool, error) {
			return tk.GetObject() == "document:1" || tk.GetObject() == "document:3", nil
		}

		reader := pipeline.NewValidatingStore(store, "store-1", pipeline.WithStoreValidator(validator))

		var got []string
		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			item, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
			if item.Err != nil {
				t.Fatalf("unexpected error: %v", item.Err)
			}
			got = append(got, item.Value)
		}

		want := []string{"document:1", "document:3"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("context cancellation stops iteration", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		tuples := []*openfgav1.Tuple{
			{Key: &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}},
		}

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		count := 0
		receiver := reader.Read(ctx, pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			_, ok := receiver.Recv(ctx)
			if !ok {
				break
			}
			count++
		}

		if count > 0 {
			t.Fatalf("expected no items from cancelled context, got %d", count)
		}
	})

	t.Run("user filter parses userset notation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1",
				gomock.Cond(func(x any) bool {
					f, ok := x.(storage.ReadStartingWithUserFilter)
					if !ok || len(f.UserFilter) != 1 {
						return false
					}
					return f.UserFilter[0].GetObject() == "group:eng" &&
						f.UserFilter[0].GetRelation() == "member"
				}),
				gomock.Any(),
			).
			Return(storage.NewStaticTupleIterator(nil), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"group:eng#member"},
		})
		defer receiver.Close()

		for {
			_, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
		}
	})

	t.Run("consistency option reaches storage", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(),
				gomock.Cond(func(x any) bool {
					opts, ok := x.(storage.ReadStartingWithUserOptions)
					if !ok {
						return false
					}
					return opts.Consistency.Preference == openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY
				}),
			).
			Return(storage.NewStaticTupleIterator(nil), nil)

		reader := pipeline.NewValidatingStore(store, "store-1",
			pipeline.WithStoreConsistency(openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY),
		)

		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		for {
			_, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
		}
	})

	t.Run("multiple users are all included in storage filter", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1",
				gomock.Cond(func(x any) bool {
					f, ok := x.(storage.ReadStartingWithUserFilter)
					if !ok || len(f.UserFilter) != 2 {
						return false
					}
					// First user is a direct user, second is a userset.
					return f.UserFilter[0].GetObject() == "user:alice" &&
						f.UserFilter[0].GetRelation() == "" &&
						f.UserFilter[1].GetObject() == "group:eng" &&
						f.UserFilter[1].GetRelation() == "member"
				}),
				gomock.Any(),
			).
			Return(storage.NewStaticTupleIterator(nil), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice", "group:eng#member"},
		})
		defer receiver.Close()

		for {
			_, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
		}
	})

	t.Run("conditions are passed to storage filter", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1",
				gomock.Cond(func(x any) bool {
					f, ok := x.(storage.ReadStartingWithUserFilter)
					if !ok {
						return false
					}
					return slices.Equal(f.Conditions, []string{"ipaddr"})
				}),
				gomock.Any(),
			).
			Return(storage.NewStaticTupleIterator(nil), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
			Conditions: []string{"ipaddr"},
		})
		defer receiver.Close()

		for {
			_, ok := receiver.Recv(context.Background())
			if !ok {
				break
			}
		}
	})

	t.Run("early break from iteration does not leak", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := mocks.NewMockRelationshipTupleReader(ctrl)

		tuples := []*openfgav1.Tuple{
			{Key: &openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"}},
			{Key: &openfgav1.TupleKey{Object: "document:3", Relation: "viewer", User: "user:alice"}},
		}

		store.EXPECT().
			ReadStartingWithUser(gomock.Any(), "store-1", gomock.Any(), gomock.Any()).
			Return(storage.NewStaticTupleIterator(tuples), nil)

		reader := pipeline.NewValidatingStore(store, "store-1")

		var got []string
		receiver := reader.Read(context.Background(), pipeline.ObjectQuery{
			ObjectType: "document",
			Relation:   "viewer",
			Users:      []string{"user:alice"},
		})
		defer receiver.Close()

		item, ok := receiver.Recv(context.Background())
		if !ok {
			t.Fatal("unexpected termination")
		}
		if item.Err != nil {
			t.Fatalf("unexpected error: %v", item.Err)
		}
		got = append(got, item.Value)

		if len(got) != 1 || got[0] != "document:1" {
			t.Fatalf("got %v, want [document:1]", got)
		}
	})
}
