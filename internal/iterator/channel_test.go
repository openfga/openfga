package iterator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestDrain(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name      string
		setupChan func(*gomock.Controller) <-chan *Msg
		expectNil bool
	}{
		{
			name: "should_drain_channel_with_iterators",
			setupChan: func(ctrl *gomock.Controller) <-chan *Msg {
				ch := make(chan *Msg, 3)
				for i := 0; i < 3; i++ {
					iter := mocks.NewMockIterator[string](ctrl)
					iter.EXPECT().Stop().Times(1)
					ch <- &Msg{Iter: iter}
				}
				close(ch)
				return ch
			},
			expectNil: false,
		},
		{
			name: "should_handle_nil_channel",
			setupChan: func(ctrl *gomock.Controller) <-chan *Msg {
				return nil
			},
			expectNil: true,
		},
		{
			name: "should_handle_empty_channel",
			setupChan: func(ctrl *gomock.Controller) <-chan *Msg {
				ch := make(chan *Msg)
				close(ch)
				return ch
			},
			expectNil: false,
		},
		{
			name: "should_handle_messages_with_nil_iterator",
			setupChan: func(ctrl *gomock.Controller) <-chan *Msg {
				ch := make(chan *Msg, 2)
				ch <- &Msg{Iter: nil}
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Stop().Times(1)
				ch <- &Msg{Iter: iter}
				close(ch)
				return ch
			},
			expectNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ch := tt.setupChan(ctrl)
			wg := Drain(ch)

			if tt.expectNil {
				require.Nil(t, wg)
			} else {
				require.NotNil(t, wg)
				wg.Wait()
			}
		})
	}
}

func TestToChannel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name           string
		setupIter      func(*gomock.Controller) storage.Iterator[string]
		batchSize      int
		expectedValues []string
		expectedErrors []error
	}{
		{
			name: "should_send_values_through_channel",
			setupIter: func(ctrl *gomock.Controller) storage.Iterator[string] {
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("value1", nil)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("value2", nil)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("value3", nil)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("", storage.ErrIteratorDone)
				return iter
			},
			batchSize:      10,
			expectedValues: []string{"value1", "value2", "value3"},
			expectedErrors: []error{nil, nil, nil},
		},
		{
			name: "should_handle_iterator_error",
			setupIter: func(ctrl *gomock.Controller) storage.Iterator[string] {
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("value1", nil)
				expectedErr := errors.New("test error")
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("", expectedErr)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("", storage.ErrIteratorDone)
				return iter
			},
			batchSize:      10,
			expectedValues: []string{"value1", ""},
			expectedErrors: []error{nil, errors.New("test error")},
		},
		{
			name: "should_stop_on_iterator_done",
			setupIter: func(ctrl *gomock.Controller) storage.Iterator[string] {
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("", storage.ErrIteratorDone)
				return iter
			},
			batchSize:      10,
			expectedValues: []string{},
			expectedErrors: []error{},
		},
		{
			name: "should_stop_on_context_cancelled",
			setupIter: func(ctrl *gomock.Controller) storage.Iterator[string] {
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Next(gomock.Any()).Times(1).Return("", context.Canceled)
				return iter
			},
			batchSize:      10,
			expectedValues: []string{},
			expectedErrors: []error{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			iter := tt.setupIter(ctrl)
			ch := ToChannel(ctx, iter, tt.batchSize)

			var values []string
			var errs []error
			for msg := range ch {
				values = append(values, msg.Value)
				errs = append(errs, msg.Err)
			}

			require.Len(t, values, len(tt.expectedValues))
			for i, expected := range tt.expectedValues {
				require.Equal(t, expected, values[i])
			}

			require.Len(t, errs, len(tt.expectedErrors))
			for i, expectedErr := range tt.expectedErrors {
				if expectedErr != nil {
					require.Error(t, errs[i])
					require.Equal(t, expectedErr.Error(), errs[i].Error())
				} else {
					require.NoError(t, errs[i])
				}
			}
		})
	}
}

func TestToChannelWithContextCancellation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	iter := mocks.NewMockIterator[string](ctrl)
	iter.EXPECT().Next(gomock.Any()).Times(1).Return("value1", nil)
	iter.EXPECT().Next(gomock.Any()).MinTimes(1).DoAndReturn(func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	ch := ToChannel[string](ctx, iter, 10)

	msg := <-ch
	require.Equal(t, "value1", msg.Value)
	require.NoError(t, msg.Err)

	cancel()

	// Channel should close after context cancellation
	timeout := time.After(1 * time.Second)
	select {
	case _, ok := <-ch:
		if ok {
			// Drain remaining messages
			for range ch {
			}
		}
	case <-timeout:
		t.Fatal("channel did not close after context cancellation")
	}
}

func TestChannelIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name           string
		setupChannel   func(*gomock.Controller) chan *Msg
		expectedValues []string
		expectedErrors []error
	}{
		{
			name: "should_iterate_through_single_iterator",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg, 1)
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Head(gomock.Any()).Return("a", nil).AnyTimes()
				iter.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter.EXPECT().Next(gomock.Any()).Return("b", nil)
				iter.EXPECT().Next(gomock.Any()).Return("c", nil)
				iter.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter.EXPECT().Stop().Times(1)
				ch <- &Msg{Iter: iter}
				close(ch)
				return ch
			},
			expectedValues: []string{"a", "b", "c"},
			expectedErrors: []error{},
		},
		{
			name: "should_chain_multiple_iterators",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg, 3)

				iter1 := mocks.NewMockIterator[string](ctrl)
				iter1.EXPECT().Head(gomock.Any()).Return("a", nil).AnyTimes()
				iter1.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("b", nil)
				iter1.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter1.EXPECT().Stop().Times(1)

				iter2 := mocks.NewMockIterator[string](ctrl)
				iter2.EXPECT().Head(gomock.Any()).Return("c", nil).AnyTimes()
				iter2.EXPECT().Next(gomock.Any()).Return("c", nil)
				iter2.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter2.EXPECT().Stop().Times(1)

				iter3 := mocks.NewMockIterator[string](ctrl)
				iter3.EXPECT().Head(gomock.Any()).Return("d", nil).AnyTimes()
				iter3.EXPECT().Next(gomock.Any()).Return("d", nil)
				iter3.EXPECT().Next(gomock.Any()).Return("e", nil)
				iter3.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter3.EXPECT().Stop().Times(1)

				ch <- &Msg{Iter: iter1}
				ch <- &Msg{Iter: iter2}
				ch <- &Msg{Iter: iter3}
				close(ch)
				return ch
			},
			expectedValues: []string{"a", "b", "c", "d", "e"},
			expectedErrors: []error{},
		},
		{
			name: "should_handle_error_in_iterator",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg, 1)
				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Head(gomock.Any()).Return("a", nil).AnyTimes()
				iter.EXPECT().Next(gomock.Any()).Return("a", nil)
				expectedErr := errors.New("iterator error")
				iter.EXPECT().Next(gomock.Any()).Return("", expectedErr)
				iter.EXPECT().Stop().Times(1)
				ch <- &Msg{Iter: iter}
				close(ch)
				return ch
			},
			expectedValues: []string{"a"},
			expectedErrors: []error{errors.New("iterator error")},
		},
		{
			name: "should_handle_error_message",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg, 1)
				ch <- &Msg{Err: errors.New("channel error")}
				close(ch)
				return ch
			},
			expectedErrors: []error{errors.New("channel error")},
		},
		{
			name: "should_skip_nil_iterators",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg, 2)
				ch <- &Msg{Iter: nil}

				iter := mocks.NewMockIterator[string](ctrl)
				iter.EXPECT().Head(gomock.Any()).Return("a", nil).AnyTimes()
				iter.EXPECT().Next(gomock.Any()).Return("a", nil)
				iter.EXPECT().Next(gomock.Any()).Return("", storage.ErrIteratorDone)
				iter.EXPECT().Stop().Times(1)
				ch <- &Msg{Iter: iter}
				close(ch)
				return ch
			},
			expectedValues: []string{"a"},
			expectedErrors: []error{},
		},
		{
			name: "should_return_done_on_empty_channel",
			setupChannel: func(ctrl *gomock.Controller) chan *Msg {
				ch := make(chan *Msg)
				close(ch)
				return ch
			},
			expectedErrors: []error{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ch := tt.setupChannel(ctrl)
			iter := FromChannel(ch)
			defer iter.Stop()

			ctx := context.Background()
			var values []string
			var errs []error

			for {
				val, err := iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					errs = append(errs, err)
					break
				}
				values = append(values, val)
			}

			require.Equal(t, tt.expectedValues, values)
			require.Len(t, errs, len(tt.expectedErrors))
			for i, expectedErr := range tt.expectedErrors {
				if expectedErr != nil {
					require.Error(t, errs[i])
					require.Equal(t, expectedErr.Error(), errs[i].Error())
				} else {
					require.NoError(t, errs[i])
				}
			}
		})
	}
}
