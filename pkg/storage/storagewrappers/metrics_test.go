package storagewrappers

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

func TestMetricsOpenFGAStorage(t *testing.T) {
	const clonesCount = 100
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().Read(gomock.Any(), "", nil, storage.ReadOptions{}).Return(nil, nil).Times(clonesCount)
	dut := NewMetricsOpenFGAStorage(mockDatastore)

	var wg sync.WaitGroup
	wg.Add(clonesCount)

	for range clonesCount {
		go func() {
			defer wg.Done()
			_, _ = dut.Read(context.Background(), "", nil, storage.ReadOptions{})
		}()
	}

	wg.Wait()

	require.Equal(t, clonesCount, dut.GetMetrics().DatastoreQueryCount)
}
