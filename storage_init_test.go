package pivot_test

import (
	"os"
	"testing"

	"github.com/benitogf/ko"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestStorageInitBeforeOnStart verifies that storage is fully initialized
// before OnStart callback is invoked, especially when using embedded storage.
// This reproduces a production issue where pivot.SyncAll() in OnStart
// panicked because the embedded leveldb wasn't started yet.
func TestStorageInitBeforeOnStart(t *testing.T) {
	// Clean up test database
	dbPath := "test_storage_init_db"
	defer os.RemoveAll(dbPath)

	// Create embedded storage (like production code does)
	embedded := &ko.EmbeddedStorage{Path: dbPath}
	dataStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: embedded,
	})

	// Create server with storage NOT started (server.Start should do it)
	server := &ooo.Server{
		Silence: true,
		Static:  true,
		Storage: dataStorage,
		Router:  mux.NewRouter(),
	}

	// Setup pivot with AutoSyncOnStart=true - this sets up OnStart callback
	// that will call SyncAll() which accesses storage
	server = pivot.Setup(server, pivot.Config{
		Keys: []pivot.Key{
			{Path: "items/*"},
		},
		NodesKey:        "devices/*",
		ClusterURL:      "127.0.0.1:19999", // Non-empty = node server
		AutoSyncOnStart: true,
	})

	// Start server - this should:
	// 1. Start storage in waitListen()
	// 2. Wait for storage to be active
	// 3. Call OnStart() which triggers SyncAll()
	// If storage isn't started before OnStart, this will panic
	server.Start("127.0.0.1:0")
	defer server.Close(os.Interrupt)

	// Verify server started successfully
	require.True(t, server.Active())
	require.True(t, server.Storage.Active())
}

// TestStorageInitMultipleStorages mimics the bundle's production setup
// where there are TWO separate storages: authStorage (started manually)
// and dataStorage (started by server.Start).
func TestStorageInitMultipleStorages(t *testing.T) {
	// Clean up test databases
	authDbPath := "test_auth_db"
	dataDbPath := "test_data_db"
	defer os.RemoveAll(authDbPath)
	defer os.RemoveAll(dataDbPath)

	// Auth storage - created and started BEFORE pivot.Setup (like bundle does)
	authEmbedded := &ko.EmbeddedStorage{Path: authDbPath}
	authStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: authEmbedded,
	})
	err := authStorage.Start(storage.Options{})
	require.NoError(t, err)
	defer authStorage.Close()

	// Data storage - NOT started (server.Start should do it)
	dataEmbedded := &ko.EmbeddedStorage{Path: dataDbPath}
	dataStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: dataEmbedded,
	})

	// Create server with data storage
	server := &ooo.Server{
		Silence: true,
		Static:  true,
		Storage: dataStorage,
		Router:  mux.NewRouter(),
	}

	// Setup pivot with mixed storages - some keys use authStorage, some use server.Storage
	server = pivot.Setup(server, pivot.Config{
		Keys: []pivot.Key{
			{Path: "tables/*"}, // Uses server.Storage (not started yet)
			{Path: "users/*", Database: authStorage, ClusterURL: "127.0.0.1:19998"}, // Uses authStorage (started)
		},
		NodesKey:        "devices/*", // Uses server.Storage
		ClusterURL:      "127.0.0.1:19999",
		AutoSyncOnStart: true,
	})

	// Start server
	server.Start("127.0.0.1:0")
	defer server.Close(os.Interrupt)

	// Verify both storages are active
	require.True(t, server.Active())
	require.True(t, server.Storage.Active())
	require.True(t, authStorage.Active())
}

// TestAttachAlreadyStartedStorage verifies that Attach() works correctly
// when called on a storage that was already started manually.
// This reproduces a production bug where:
// 1. authStorage.Start() was called in main.go
// 2. pivot.GetInstance(server).Attach(authStorage) called Start() again
// 3. The double-start corrupted the leveldb handle causing nil pointer panic
func TestAttachAlreadyStartedStorage(t *testing.T) {
	monotonic.Init()

	// Clean up test databases
	authDbPath := "test_attach_auth_db"
	dataDbPath := "test_attach_data_db"
	defer os.RemoveAll(authDbPath)
	defer os.RemoveAll(dataDbPath)

	// Auth storage - created and started BEFORE pivot.Setup (like bundle does)
	authEmbedded := &ko.EmbeddedStorage{Path: authDbPath}
	authStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: authEmbedded,
	})
	// Start auth storage manually BEFORE server starts
	err := authStorage.Start(storage.Options{})
	require.NoError(t, err)
	defer authStorage.Close()

	// Write some test data to auth storage
	_, err = authStorage.Set("users/test1", []byte(`{"name":"alice"}`))
	require.NoError(t, err)

	// Data storage - NOT started (server.Start will do it)
	dataEmbedded := &ko.EmbeddedStorage{Path: dataDbPath}
	dataStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: dataEmbedded,
	})

	// Create server
	server := &ooo.Server{
		Silence: true,
		Static:  true,
		Storage: dataStorage,
		Router:  mux.NewRouter(),
	}

	// Setup pivot - this does NOT call Attach yet
	server = pivot.Setup(server, pivot.Config{
		Keys: []pivot.Key{
			{Path: "tables/*"},
		},
		NodesKey:        "devices/*",
		ClusterURL:      "127.0.0.1:19999",
		AutoSyncOnStart: true,
	})

	// Start server
	server.Start("127.0.0.1:0")
	defer server.Close(os.Interrupt)

	// Now call Attach on the ALREADY STARTED authStorage
	// This was the bug - Attach would call Start() again, corrupting the leveldb
	instance := pivot.GetInstance(server)
	require.NotNil(t, instance)

	err = instance.Attach(authStorage)
	require.NoError(t, err)

	// Verify auth storage still works after Attach
	// Before the fix, this would panic with nil pointer dereference
	require.True(t, authStorage.Active())

	// Verify we can still read from auth storage
	data, err := authStorage.Get("users/test1")
	require.NoError(t, err)
	require.Contains(t, string(data.Data), "alice")

	// Verify we can still write to auth storage
	_, err = authStorage.Set("users/test2", []byte(`{"name":"bob"}`))
	require.NoError(t, err)

	data, err = authStorage.Get("users/test2")
	require.NoError(t, err)
	require.Contains(t, string(data.Data), "bob")
}
