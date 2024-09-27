package storagewrappers

import (
	"context"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"reflect"
	"testing"
)

var (
	testTuples = map[string]*openfgav1.Tuple{
		"user:11#member#group:1": {
			Key: &openfgav1.TupleKey{
				User:     "user:11",
				Relation: "member",
				Object:   "group:1",
			},
		},
		"user:12#member#group:1": {
			Key: &openfgav1.TupleKey{
				User:     "user:12",
				Relation: "member",
				Object:   "group:1",
			},
		},
		"user:13#member#group:1": {
			Key: &openfgav1.TupleKey{
				User:     "user:13",
				Relation: "member",
				Object:   "group:1",
			},
		},
		"user:21#member#group:2": {
			Key: &openfgav1.TupleKey{
				User:     "user:21",
				Relation: "member",
				Object:   "group:2",
			},
		},
		"user:22#member#group:2": {
			Key: &openfgav1.TupleKey{
				User:     "user:22",
				Relation: "member",
				Object:   "group:2",
			},
		},
	}
)

func TestNewCombinedTupleReader(t *testing.T) {
	controller := &gomock.Controller{T: t}
	reader := mocks.NewMockRelationshipTupleReader(controller)
	tuples := []*openfgav1.TupleKey{
		testTuples["user:11#member#group:1"].Key,
	}
	assert.Equal(t, NewCombinedTupleReader(reader, tuples), &combinedTupleReader{
		RelationshipTupleReader: reader,
		contextualTuples:        tuples,
	})
}

func Test_combinedTupleReader_Read(t *testing.T) {
	controller := gomock.NewController(t)

	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

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
					testTuples["user:11#member#group:1"].Key,
					testTuples["user:12#member#group:1"].Key,
					testTuples["user:21#member#group:2"].Key,
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
					testTuples["user:13#member#group:1"],
					testTuples["user:22#member#group:2"],
				},
			},
			want: []*openfgav1.Tuple{
				testTuples["user:11#member#group:1"],
				testTuples["user:12#member#group:1"],
				testTuples["user:13#member#group:1"],
				testTuples["user:22#member#group:2"],
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
					testTuples["user:13#member#group:1"],
					testTuples["user:22#member#group:2"],
				},
			},
			want: []*openfgav1.Tuple{
				testTuples["user:13#member#group:1"],
				testTuples["user:22#member#group:2"],
			},
			wantErr: false,
		},
		{
			name: "Test combinedTupleReader Read OK - no testTuples read",
			fields: fields{
				RelationshipTupleReader: mockRelationshipTupleReader,
				contextualTuples: []*openfgav1.TupleKey{
					testTuples["user:11#member#group:1"].Key,
					testTuples["user:12#member#group:1"].Key,
					testTuples["user:21#member#group:2"].Key,
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
				testTuples["user:11#member#group:1"],
				testTuples["user:12#member#group:1"],
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
	controller := gomock.NewController(t)

	mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(controller)

	c := &combinedTupleReader{
		RelationshipTupleReader: mockRelationshipTupleReader,
		contextualTuples: []*openfgav1.TupleKey{
			testTuples["user:11#member#group:1"].Key,
			testTuples["user:12#member#group:1"].Key,
		},
	}

	mockRelationshipTupleReader.EXPECT().
		ReadPage(context.Background(), "1", testTuples["user:11#member#group:1"].Key, storage.ReadPageOptions{}).
		Return([]*openfgav1.Tuple{
			{
				Key: &openfgav1.TupleKey{
					Relation: "member",
					Object:   "group:1",
				},
			},
		}, nil, nil)

	got, _, err := c.ReadPage(context.Background(), "1", testTuples["user:11#member#group:1"].Key, storage.ReadPageOptions{})
	if (err != nil) != false {
		t.Errorf("ReadPage() error = %v", err)
		return
	}
	if !reflect.DeepEqual(got, []*openfgav1.Tuple{
		{
			Key: &openfgav1.TupleKey{
				Relation: "member",
				Object:   "group:1",
			},
		},
	}) {
		t.Errorf("ReadPage() got = %v", got)
	}
}

func Test_combinedTupleReader_ReadStartingWithUser(t *testing.T) {
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
		want    storage.TupleIterator
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &combinedTupleReader{
				RelationshipTupleReader: tt.fields.RelationshipTupleReader,
				contextualTuples:        tt.fields.contextualTuples,
			}
			got, err := c.ReadStartingWithUser(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadStartingWithUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadStartingWithUser() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_combinedTupleReader_ReadUserTuple(t *testing.T) {
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
		want    *openfgav1.Tuple
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &combinedTupleReader{
				RelationshipTupleReader: tt.fields.RelationshipTupleReader,
				contextualTuples:        tt.fields.contextualTuples,
			}
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
		want    storage.TupleIterator
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &combinedTupleReader{
				RelationshipTupleReader: tt.fields.RelationshipTupleReader,
				contextualTuples:        tt.fields.contextualTuples,
			}
			got, err := c.ReadUsersetTuples(tt.args.ctx, tt.args.store, tt.args.filter, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadUsersetTuples() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadUsersetTuples() got = %v, want %v", got, tt.want)
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
			User:      "user:testUser1",
			Relation:  "member",
			Object:    "group:1",
			Condition: nil,
		},
		{
			User:      "user:testUser2",
			Relation:  "owner",
			Object:    "doc:2",
			Condition: nil,
		},
		{
			User:      "user:testUser2",
			Relation:  "member",
			Object:    "group:1",
			Condition: nil,
		},
		{
			User:      "user:testUser3",
			Relation:  "viewer",
			Object:    "doc:3",
			Condition: nil,
		},
		{
			User:      "user:testUser3",
			Relation:  "member",
			Object:    "group:1",
			Condition: nil,
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
				{
					Key: &openfgav1.TupleKey{
						User:     "user:testUser1",
						Relation: "member",
						Object:   "group:1",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						User:     "user:testUser2",
						Relation: "member",
						Object:   "group:1",
					},
				},
				{
					Key: &openfgav1.TupleKey{
						User:     "user:testUser3",
						Relation: "member",
						Object:   "group:1",
					},
				},
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
