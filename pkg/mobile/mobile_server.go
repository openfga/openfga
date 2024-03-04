package mobile

import (
	"sync"

	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
)

var lock = &sync.Mutex{}

var serverInstance *server.Server

func getInstance() *server.Server {
	if serverInstance == nil {
		lock.Lock()

		defer lock.Unlock()

		if serverInstance == nil {
			dsCfg := sqlcommon.NewConfig()
			datastore, error := sqlite.New("file:test.db", dsCfg)

			if error != nil {
				panic(error)
			}

			serverInstance = server.MustNewServerWithOpts(
				server.WithDatastore(datastore),
			)
		}
	}

	return serverInstance
}
