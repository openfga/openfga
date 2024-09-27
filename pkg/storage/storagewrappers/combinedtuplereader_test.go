package storagewrappers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/tuple"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
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
		} {
			result[key] = &openfgav1.Tuple{Key: parseTuple(key)}
		}
		return result
	}()
)

// parseTuple parses a tuple string into a TupleKey and panics if there is an error.
var parseTuple = func(tupleString string) *openfgav1.TupleKey {
	tupleKey, err := tuple.ParseTupleString(tupleString)
	if err != nil {
		panic(err)
	}
	return tupleKey
}

// makeMocks creates mocks for the RelationshipTupleReader.
func makeMocks(t *testing.T) *mocks.MockRelationshipTupleReader {
	controller := gomock.NewController(t)

	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

	return mockRelationshipTupleReader
}

func Test_combinedTupleReader_Read(t *testing.T) {
	mockRelationshipTupleReader := makeMocks(t)

	type fields struct {
		RelationshipTupleReader storage.RelationshipTupleReader
		contextualTuples        []*openfgav1.TupleKey
	}
	type args struct {
		ctx     context.Context
		storeID string
		tk      *openfgav1.TupleKey
		options storage.ReadOptions
		tuples  []*openfgav1.Tuple
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*openfgav1.Tuple
		wantErr bool
	}{
		{
			name: "Test combinedTupleReader Read OK",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
				},
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				tk: &openfgav1.TupleKey{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
				tuples: []*openfgav1.Tuple{
					testTuples["group:1#member@user:13"],
					testTuples["group:2#member@user:22"],
				},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:1#member@user:12"],
				testTuples["group:1#member@user:13"],
				testTuples["group:2#member@user:22"],
			},
			wantErr: false,
		},
		{
			name: "Test combinedTupleReader Read OK - no contextual testTuples",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples:        []*openfgav1.TupleKey{},
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				tk: &openfgav1.TupleKey{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
				tuples: []*openfgav1.Tuple{
					testTuples["group:1#member@user:13"],
					testTuples["group:2#member@user:22"],
				},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:13"],
				testTuples["group:2#member@user:22"],
			},
			wantErr: false,
		},
		{
			name: "Test combinedTupleReader Read OK - no testTuples read",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
					testTuples["group:1#member@user:12"].GetKey(),
					testTuples["group:2#member@user:21"].GetKey(),
				},
			},
			args: args{
				ctx:     context.Background(),
				storeID: "1",
				tk: &openfgav1.TupleKey{
					Relation: "member",
					Object:   "group:1",
				},
				options: storage.ReadOptions{},
				tuples:  []*openfgav1.Tuple{},
			},
			want: []*openfgav1.Tuple{
				testTuples["group:1#member@user:11"],
				testTuples["group:1#member@user:12"],
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)

			mockRelationshipTupleReader.
				EXPECT().
				Read(tt.args.ctx, tt.args.storeID, tt.args.tk, tt.args.options).
				Return(storage.NewStaticTupleIterator(tt.args.tuples), nil). // no error
				Times(1)

			got, err := c.Read(tt.args.ctx, tt.args.storeID, tt.args.tk, tt.args.options)

			gotArr := make([]*openfgav1.Tuple, 0)
			for {
				if tuple, err := got.Next(tt.args.ctx); err == nil {
					gotArr = append(gotArr, tuple)
				} else {
					break
				}
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.want, gotArr)
		})
	}
}

func Test_combinedTupleReader_ReadPage(t *testing.T) {
	mockRelationshipTupleReader := makeMocks(t)

	c := NewCombinedTupleReader(mockRelationshipTupleReader, []*openfgav1.TupleKey{
		testTuples["group:1#member@user:11"].GetKey(),
		testTuples["group:1#member@user:12"].GetKey(),
	})

	mockRelationshipTupleReader.EXPECT().
		ReadPage(context.Background(), "1", testTuples["group:1#member@user:11"].GetKey(), storage.ReadPageOptions{}).
		Return([]*openfgav1.Tuple{testTuples["group:1#member@user:11"]}, nil, nil)

	got, _, err := c.ReadPage(context.Background(), "1", testTuples["group:1#member@user:11"].GetKey(), storage.ReadPageOptions{})
	require.NoError(t, err)

	if !reflect.DeepEqual(got, []*openfgav1.Tuple{testTuples["group:1#member@user:11"]}) {
		t.Errorf("ReadPage() got = %v", got)
	}
}

func Test_combinedTupleReader_ReadStartingWithUser(t *testing.T) {
	mockRelationshipTupleReader := makeMocks(t)
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
			name: "Test combinedTupleReader ReadStartingWithUser OK",
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
			name: "Test combinedTupleReader ReadStartingWithUser OK - no contextual tuples",
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
			name: "Test combinedTupleReader ReadStartingWithUser OK - no relationship tuples",
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
			name: "Test combinedTupleReader ReadStartingWithUser OK - type mismatch",
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
			name: "Test combinedTupleReader ReadStartingWithUser OK - relation mismatch",
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
			name: "Test combinedTupleReader ReadStartingWithUser error - relationship tuple reader error",
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
			c := &combinedTupleReader{
				RelationshipTupleReader: tt.fields.RelationshipTupleReader,
				contextualTuples:        tt.fields.contextualTuples,
			}
			got, err := c.ReadStartingWithUser(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if tt.wantErr != nil {
				assert.EqualErrorf(t, tt.wantErr, err.Error(), "ReadStartingWithUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotArr := make([]*openfgav1.Tuple, 0)
			for {
				if tuple, err := got.Next(tt.args.ctx); err == nil {
					gotArr = append(gotArr, tuple)
				} else {
					break
				}
			}
			if !reflect.DeepEqual(gotArr, tt.want) {
				t.Errorf("ReadStartingWithUser() got = %v, want %v", gotArr, tt.want)
			}
		})
	}
}

func Test_combinedTupleReader_ReadUserTuple(t *testing.T) {
	controller := gomock.NewController(t)
	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

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
		name    string
		fields  fields
		args    args
		setups  func()
		want    *openfgav1.Tuple
		wantErr bool
	}{
		{
			name: "Test combinedTupleReader ReadUserTuple OK - contextual tuple found",
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
			want:    testTuples["group:1#member@user:11"],
			wantErr: false,
		},
		{
			name: "Test combinedTupleReader ReadUserTuple OK - contextual tuple not found",
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
			want:    testTuples["group:1#member@user:13"],
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setups != nil {
				tt.setups()
			}
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)

			got, err := c.ReadUserTuple(tt.args.ctx, tt.args.store, tt.args.tk, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadUserTuple() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadUserTuple() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_combinedTupleReader_ReadUsersetTuples(t *testing.T) {
	mockRelationshipTupleReader := makeMocks(t)
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
			name: "Test combinedTupleReader ReadUsersetTuples OK - no userset",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["group:1#member@user:11"].GetKey(),
				},
			},
			args: args{
				ctx:     context.Background(),
				store:   "1",
				filter:  storage.ReadUsersetTuplesFilter{},
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
			name: "Test combinedTupleReader ReadUsersetTuples OK - userset",
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
			name: "Test combinedTupleReader ReadUsersetTuples error - relationship tuple reader error",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCombinedTupleReader(tt.fields.RelationshipTupleReader, tt.fields.contextualTuples)
			if tt.setup != nil {
				tt.setup()
			}
			got, err := c.ReadUsersetTuples(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if err != nil {
				assert.EqualErrorf(t, tt.wantErr, err.Error(), "ReadUsersetTuples() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
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
			name: "Test filterTuples OK",
			args: args{
				tuples:         okTuples,
				targetObject:   "group:1",
				targetRelation: "member",
			},
			want: []*openfgav1.Tuple{
				{Key: parseTuple("group:1#member@user:testUser1")},
				{Key: parseTuple("group:1#member@user:testUser2")},
				{Key: parseTuple("group:1#member@user:testUser3")},
			},
		},
		{
			name: "Test filterTuples with incomplete testTuples",
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
			name: "Test filterTuples with incomplete testTuples - only relation",
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
						User:     "user:testUser2",
						Relation: "member",
					},
				},
			},
		},
		{
			name: "Test filterTuples with incomplete testTuples - only object",
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
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterTuples(tt.args.tuples, tt.args.targetObject, tt.args.targetRelation); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterTuples() = %v, want %v", got, tt.want)
			}
		})
	}
}
