package iterator

import (
	"context"

	"github.com/openfga/openfga/pkg/storage"
)

func SkipTo(ctx context.Context, iter storage.Iterator[string], target string) error {
	for {
		t, err := iter.Head(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				return nil
			}
			return err
		}

		// If current head >= target, we're done
		if t >= target {
			return nil
		}

		// Otherwise advance the iterator
		_, err = iter.Next(ctx)
		if err != nil {
			if storage.IterIsDoneOrCancelled(err) {
				return nil
			}
			return err
		}
	}
}
