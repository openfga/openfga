package storagewrappers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	testTuples = func() map[string]*openfgav1.Tuple {
		result := make(map[string]*openfgav1.Tuple)
		for _, key := range []string{
			"group:1#member@user:11",
			"group:1#member@user:12",
			"group:1#member@user:13",
			"group:2#member@user:21",
			"group:2#member@user:22",
			"group:3#member@user:11",
			// userset tuples
			"folder:backlog#viewer@group:1#member",
			"folder:backlog#viewer@group:*",
		} {
			result[key] = &openfgav1.Tuple{Key: tuple.MustParseTupleString(key)}
		}
		return result
	}()
)

// makeMocks creates mocks for the RelationshipTupleReader.
func makeMocks(t *testing.T) (*gomock.Controller, *mocks.MockRelationshipTupleReader) {
	controller := gomock.NewController(t)

	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

	return controller, mockRelationshipTupleReader
}

func Test_combinedTupleReader_Constructor(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

	c := NewCombinedTupleReader(mockRelationshipTupleReader, tuple.MustParseTupleStrings(
		"group:3#member@user:maria",
		"group:2#member@user:maria",
		"group:1#member@user:maria",
	))

	require.Equal(t, "group:1", c.contextualTuplesOrderedByObjectID[0].GetObject())
	require.Equal(t, "group:2", c.contextualTuplesOrderedByObjectID[1].GetObject())
	require.Equal(t, "group:3", c.contextualTuplesOrderedByObjectID[2].GetObject())
}

func Test_combinedTupleReader_Read(t *testing.T) {
	mockCtl, mockRelationshipTupleReader := makeMocks(t)
	defer mockCtl.Finish()

	type fields struct {
		RelationshipTupleReader storage.RelationshipTupleReader
		contextualTuples        []*openfgav1.TupleKey
	}
	type args struct {
		ctx     context.Context
		storeID string
		filter  *storage.ReadFilter
		options storage.ReadOptions
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*openfgav1.Tuple
		wantErr error
		setup   func()
	}{
		{
			name: "Test_combinedTupleReader_Read_OK",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: tuple.MustParseTupleStrings(
					"group:1#member@user:11",
					"group:1#member@user:12",
					"group:2#member@user:21",
				),
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				filter: &storage.ReadFilter{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:1#member@user:12"],
				testTuples["group:1#member@user:13"],
				testTuples["group:2#member@user:22"],
			},
			wantErr: nil,
			setup: func() {
				mockRelationshipTupleReader.EXPECT().
					Read(gomock.Any(), "1", storage.ReadFilter{Relation: "member", Object: "group:1"}, gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						testTuples["group:1#member@user:13"],
						testTuples["group:2#member@user:22"],
					}), nil)
			},
		},
		{
			name: "Test_combinedTupleReader_Read_OK_no_contextual_testTuples",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples:        []*openfgav1.TupleKey{},
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				filter: &storage.ReadFilter{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:13"],
				testTuples["group:2#member@user:22"],
			},
			wantErr: nil,
			setup: func() {
				mockRelationshipTupleReader.EXPECT().
					Read(gomock.Any(), "1", storage.ReadFilter{Relation: "member", Object: "group:1"}, gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						testTuples["group:1#member@user:13"],
						testTuples["group:2#member@user:22"],
					}), nil)
			},
		},
		{
			name: "Test_combinedTupleReader_Read_OK_no_testTuples_read",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: tuple.MustParseTupleStrings(
					"group:1#member@user:11",
					"group:1#member@user:12",
					"group:2#member@user:21",
				),
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				filter: &storage.ReadFilter{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:1#member@user:12"],
			},
			wantErr: nil,
			setup: func() {
				mockRelationshipTupleReader.EXPECT().
					Read(gomock.Any(), "1", storage.ReadFilter{Relation: "member", Object: "group:1"}, gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
		},
		{
			name: "Test_combinedTupleReader_Read_error_relationship_tuple_reader_error",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: tuple.MustParseTupleStrings(
					"group:1#member@user:11",
					"group:1#member@user:12",
					"group:2#member@user:21",
				),
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				filter: &storage.ReadFilter{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:1#member@user:12"],
			},
			wantErr: errors.New("test read error"),
			setup: func() {
				mockRelationshipTupleReader.EXPECT().
					Read(gomock.Any(), "1", storage.ReadFilter{Relation: "member", Object: "group:1"}, gomock.Any()).
					Return(nil, errors.New("test read error"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}

			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)

			got, err := c.Read(tt.args.ctx, tt.args.storeID, *tt.args.filter, tt.args.options)

			if tt.wantErr != nil {
				assert.EqualErrorf(t, tt.wantErr, err.Error(), "Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			gotArr := make([]*openfgav1.Tuple, 0)
			for {
				if tuple, err := got.Next(tt.args.ctx); err == nil {
					gotArr = append(gotArr, tuple)
				} else {
					break
				}
			}

			assert.Equal(t, tt.want, gotArr)
		})
	}
}

func Test_combinedTupleReader_ReadPage(t *testing.T) {
	mockCtl, mockRelationshipTupleReader := makeMocks(t)
	defer mockCtl.Finish()

	c := NewCombinedTupleReader(mockRelationshipTupleReader, tuple.MustParseTupleStrings(
		"group:1#member@user:11",
		"group:1#member@user:12",
	))

	tk := testTuples["group:1#member@user:11"].GetKey()
	filter := storage.ReadFilter{Relation: tk.GetRelation(), Object: tk.GetObject(), User: tk.GetUser()}

	mockRelationshipTupleReader.EXPECT().
		ReadPage(context.Background(), "1", filter, storage.ReadPageOptions{}).
		Return([]*openfgav1.Tuple{testTuples["group:1#member@user:11"]}, "", nil)

	got, _, err := c.ReadPage(context.Background(), "1", filter, storage.ReadPageOptions{})
	require.NoError(t, err)

	if !reflect.DeepEqual(got, []*openfgav1.Tuple{testTuples["group:1#member@user:11"]}) {
		t.Errorf("ReadPage() got = %v", got)
	}
}

func Test_combinedTupleReader_ReadStartingWithUser(t *testing.T) {
	mockCtl, mockRelationshipTupleReader := makeMocks(t)
	defer mockCtl.Finish()

	type fields struct {
		RelationshipTupleReader storage.RelationshipTupleReader
		contextualTuples        []*openfgav1.TupleKey
	}
	type args struct {
		ctx     context.Context
		store   string
		filter  storage.ReadStartingWithUserFilter
		options storage.ReadStartingWithUserOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		setups  func()
		want    []*openfgav1.Tuple
		wantErr error
	}{
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
					testTuples["group:2#member@user:22"].GetKey(),
					testTuples["group:2#member@user:23"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object: "user:11",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{testTuples["group:3#member@user:11"]}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:3#member@user:11"],
			},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK_sorted",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:maria"),
					tuple.NewTupleKey("document:3", "viewer", "user:maria"),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "document",
					Relation:   "viewer",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object: "user:maria",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{
					WithResultsSortedAscending: true,
				},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:2", "viewer", "user:maria")},
						{Key: tuple.NewTupleKey("document:4", "viewer", "user:maria")}}), nil)
			},
			want: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "viewer", "user:maria")},
				{Key: tuple.NewTupleKey("document:2", "viewer", "user:maria")},
				{Key: tuple.NewTupleKey("document:3", "viewer", "user:maria")},
				{Key: tuple.NewTupleKey("document:4", "viewer", "user:maria")},
			},
		},
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK_no_contextual_tuples",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples:        []*openfgav1.TupleKey{},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object: "user:11",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{testTuples["group:3#member@user:11"]}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["group:3#member@user:11"],
			},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK_no_relationship_tuples",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
					testTuples["group:2#member@user:22"].GetKey(),
					testTuples["group:2#member@user:23"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object: "user:11",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
			},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK_type_mismatch",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
					testTuples["group:2#member@user:22"].GetKey(),
					testTuples["group:2#member@user:23"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "folder",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object: "user:11",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_OK_relation_mismatch",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
					testTuples["group:2#member@user:22"].GetKey(),
					testTuples["group:2#member@user:23"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "owner",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object:   "user:11",
							Relation: "owner",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},

		{
			name: "Test_combinedTupleReader_ReadStartingWithUser_error_relationship_tuple_reader_error",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
					testTuples["group:2#member@user:22"].GetKey(),
					testTuples["group:2#member@user:23"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "",
				filter: storage.ReadStartingWithUserFilter{
					ObjectType: "group",
					Relation:   "member",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object:   "user:11",
							Relation: "member",
						},
					},
				},
				options: storage.ReadStartingWithUserOptions{},
			},
			setups: func() {
				mockRelationshipTupleReader.EXPECT().
					ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, errors.New("test read error"))
			},
			want:    nil,
			wantErr: errors.New("test read error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setups != nil {
				tt.setups()
			}
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)
			got, err := c.ReadStartingWithUser(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if tt.wantErr != nil {
				assert.EqualErrorf(t, tt.wantErr, err.Error(), "ReadStartingWithUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			gotArr := make([]*openfgav1.Tuple, 0)
			for {
				if tuple, err := got.Next(tt.args.ctx); err == nil {
					gotArr = append(gotArr, tuple)
				} else {
					break
				}
			}

			if diff := cmp.Diff(gotArr, tt.want, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_combinedTupleReader_ReadUserTuple(t *testing.T) {
	mockCtl, mockRelationshipTupleReader := makeMocks(t)
	defer mockCtl.Finish()

	type fields struct {
		RelationshipTupleReader storage.RelationshipTupleReader
		contextualTuples        []*openfgav1.TupleKey
	}
	type args struct {
		ctx     context.Context
		store   string
		tk      *openfgav1.TupleKey
		options storage.ReadUserTupleOptions
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		setups func()
		want   *openfgav1.Tuple
	}{
		{
			name: "Test_combinedTupleReader_ReadUserTuple_OK_contextual_tuple_found",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
				},
			},
			setups: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Times(0)
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				tk: &openfgav1.TupleKey{
					User:     "user:11",
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadUserTupleOptions{},
			},
			want: testTuples["group:1#member@user:11"],
		},
		{
			name: "Test_combinedTupleReader_ReadUserTuple_OK_contextual_tuple_not_found",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
				},
			},
			setups: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(testTuples["group:1#member@user:13"], nil)
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				tk: &openfgav1.TupleKey{
					User:     "user:13",
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadUserTupleOptions{},
			},
			want: testTuples["group:1#member@user:13"],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setups != nil {
				tt.setups()
			}
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)

			got, err := c.ReadUserTuple(tt.args.ctx, tt.args.store, tt.args.tk, tt.args.options)
			require.NoError(t, err)
			if diff := cmp.Diff(got, tt.want, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_combinedTupleReader_ReadUsersetTuples(t *testing.T) {
	mockCtl, mockRelationshipTupleReader := makeMocks(t)
	defer mockCtl.Finish()

	type fields struct {
		RelationshipTupleReader storage.RelationshipTupleReader
		contextualTuples        []*openfgav1.TupleKey
	}
	type args struct {
		ctx     context.Context
		store   string
		filter  storage.ReadUsersetTuplesFilter
		options storage.ReadUsersetTuplesOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		setup   func()
		want    []*openfgav1.Tuple
		wantErr error
	}{
		{
			name: "Test_combinedTupleReader_ReadUsersetTuples_OK_no_userset",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "group",
					Relation: "member",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.WildcardRelationReference("group"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{testTuples["group:1#member@user:12"]}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:12"],
			},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadUsersetTuples_OK_userset",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("group", "member"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{testTuples["group:1#member@user:12"]}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["folder:backlog#viewer@group:1#member"],
				testTuples["group:1#member@user:12"],
			},
			wantErr: nil,
		},
		{
			name: "Test_combinedTupleReader_ReadUsersetTuples_error_relationship_tuple_reader_error",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member#viewer#folder:backlog"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("group", "member"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, errors.New("test read error"))
			},
			want:    nil,
			wantErr: errors.New("test read error"),
		},
		{
			name: "filter_wildcard_tuple_not_wildcard",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.WildcardRelationReference("group"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "filter_wildcard_tuple_wrong_type",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:*"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.WildcardRelationReference("foo"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "filter_wildcard_tuple_matches_filter",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:*"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.WildcardRelationReference("group"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["folder:backlog#viewer@group:*"],
			},
			wantErr: nil,
		},
		{
			name: "filter_userset_tuple_not_userset",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:*"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("group", "member"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "filter_userset_tuple_not_match_type",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("other", "member"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "filter_userset_tuple_not_match_relation",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("group", "owner"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
		{
			name: "filter_userset_tuple_matches",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
						typesystem.DirectRelationReference("group", "member"),
						typesystem.DirectRelationReference("group", "other"),
					},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want: []*openfgav1.Tuple{
				testTuples["folder:backlog#viewer@group:1#member"],
			},
			wantErr: nil,
		},
		{
			name: "empty_restrictions",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["folder:backlog#viewer@group:1#member"].GetKey(),
				},
			},
			args: args{
				ctx:   context.Background(),
				store: "1",
				filter: storage.ReadUsersetTuplesFilter{
					Object:   "folder:backlog",
					Relation: "viewer",
					// this should never happen. In real life, the safe thing to do is to
					// ignore the tuple
					AllowedUserTypeRestrictions: []*openfgav1.RelationReference{},
				},
				options: storage.ReadUsersetTuplesOptions{},
			},
			setup: func() {
				mockRelationshipTupleReader.
					EXPECT().
					ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil)
			},
			want:    []*openfgav1.Tuple{},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)
			if tt.setup != nil {
				tt.setup()
			}
			got, err := c.ReadUsersetTuples(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if tt.wantErr != nil {
				assert.EqualErrorf(t, tt.wantErr, err.Error(), "ReadUsersetTuples() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			gotArr := make([]*openfgav1.Tuple, 0)
			for {
				if tuple, err := got.Next(tt.args.ctx); err == nil {
					gotArr = append(gotArr, tuple)
				} else {
					break
				}
			}
			if !reflect.DeepEqual(gotArr, tt.want) {
				t.Errorf("ReadUsersetTuples() got = %v, want %v", gotArr, tt.want)
			}
		})
	}
}

func Test_filterTuples(t *testing.T) {
	type args struct {
		tuples         []*openfgav1.TupleKey
		targetObject   string
		targetRelation string
		targetUsers    []string
	}
	okTuples := []*openfgav1.TupleKey{
		{
			User:     "user:testUser1",
			Relation: "member",
			Object:   "group:1",
		},
		{
			User:     "user:testUser2",
			Relation: "owner",
			Object:   "doc:2",
		},
		{
			User:     "user:testUser2",
			Relation: "member",
			Object:   "group:1",
		},
		{
			User:     "user:testUser3",
			Relation: "viewer",
			Object:   "doc:3",
		},
		{
			User:     "user:testUser3",
			Relation: "member",
			Object:   "group:1",
		},
	}
	incompleteTuples := []*openfgav1.TupleKey{
		{
			Object: "group:1",
		},
		{
			User: "user:testUser2",
		},
		{
			Relation: "member",
		},
		{
			Relation: "member",
			Object:   "group:1",
		},
		{
			User:   "user:testUser2",
			Object: "doc:2",
		},
		{
			User:     "user:testUser2",
			Relation: "member",
		},
		{
			Condition: &openfgav1.RelationshipCondition{
				Name: "incomplete",
			},
		},
	}
	tests := []struct {
		name string
		args args
		want []*openfgav1.Tuple
	}{
		{
			name: "Test_filterTuples_OK",
			args: args{
				tuples:         okTuples,
				targetObject:   "group:1",
				targetRelation: "member",
				targetUsers:    []string{"user:testUser1"},
			},
			want: []*openfgav1.Tuple{
				{Key: tuple.MustParseTupleString("group:1#member@user:testUser1")},
			},
		},
		{
			name: "Test_filterTuples_OK_multiple_users",
			args: args{
				tuples:         okTuples,
				targetObject:   "group:1",
				targetRelation: "member",
				targetUsers:    []string{"user:testUser1", "user:testUser2", "user:testUser3"},
			},
			want: []*openfgav1.Tuple{
				{Key: tuple.MustParseTupleString("group:1#member@user:testUser1")},
				{Key: tuple.MustParseTupleString("group:1#member@user:testUser2")},
				{Key: tuple.MustParseTupleString("group:1#member@user:testUser3")},
			},
		},
		{
			name: "Test_filterTuples_with_incomplete_testTuples",
			args: args{
				tuples:         incompleteTuples,
				targetObject:   "group:1",
				targetRelation: "member",
			},
			want: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Relation: "member",
						Object:   "group:1",
					},
				},
			},
		},
		{
			name: "Test_filterTuples_with_incomplete_testTuples_only_relation",
			args: args{
				tuples:         incompleteTuples,
				targetRelation: "member",
			},
			want: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Relation: "member",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						Relation: "member",
						Object:   "group:1",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						User:     "user:testUser2",
						Relation: "member",
					},
				},
			},
		},
		{
			name: "Test_filterTuples_with_incomplete_testTuples_only_object",
			args: args{
				tuples:       incompleteTuples,
				targetObject: "group:1",
			},
			want: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						Object: "group:1",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						Relation: "member",
						Object:   "group:1",
					},
				},
			},
		},
		{
			name: "Test_filterTuples_with_incomplete_testTuples_only_user",
			args: args{
				tuples:      incompleteTuples,
				targetUsers: []string{"user:testUser2"},
			},
			want: []*openfgav1.Tuple{
				{
					Key: &openfgav1.TupleKey{
						User: "user:testUser2",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						User:   "user:testUser2",
						Object: "doc:2",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						User:     "user:testUser2",
						Relation: "member",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterTuples(tt.args.tuples, tt.args.targetObject, tt.args.targetRelation, tt.args.targetUsers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterTuples() = %v, want %v", got, tt.want)
			}
		})
	}
}
