package iterator

import (
	"context"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
)

type dedupe struct {
	iter    storage.Iterator[string]
	mu      sync.Mutex
	last    string // GUARDED_BY(mu)
	hasLast bool   // GUARDED_BY(mu)
}

var _ storage.Iterator[string] = (*dedupe)(nil)

// Dedupe skips repeated values of a sorted iterator. NewFilteredIterator cannot be used: it has no Head, which Stream needs.
func Dedupe(iter storage.Iterator[string]) storage.Iterator[string] {
	return &dedupe{iter: iter}
}

// skipDuplicates returns inner errors as is and never retries, so a failing Head cannot loop. The caller holds mu.
func (d *dedupe) skipDuplicates(ctx context.Context) (string, error) {
	for {
		head, err := d.iter.Head(ctx)
		if err != nil {
			return "", err
		}
		if !d.hasLast || head != d.last {
			return head, nil
		}
		if _, err := d.iter.Next(ctx); err != nil {
			return "", err
		}
	}
}

// Head see [storage.Iterator.Head].
func (d *dedupe) Head(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.skipDuplicates(ctx)
}

// Next see [storage.Iterator.Next].
func (d *dedupe) Next(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.skipDuplicates(ctx); err != nil {
		return "", err
	}
	v, err := d.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	d.last, d.hasLast = v, true
	return v, nil
}

// Stop see [storage.Iterator.Stop].
func (d *dedupe) Stop() { d.iter.Stop() }

// IsOrdered forwards from the inner iterator.
func (d *dedupe) IsOrdered() bool { return d.iter.IsOrdered() }
