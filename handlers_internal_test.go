package pivot

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// failingTombstoneStorage forces Set on the pivot tombstone prefix to fail.
// VV writes (StoragePrefix+"vv/") are also under StoragePrefix, but the
// Delete handler's tombstone target is "pivot/<basekey>" exactly, so the
// match below is precise and won't perturb other writes the test makes.
type failingTombstoneStorage struct {
	storage.Database
	tombstoneKey string
	err          error
}

func (f *failingTombstoneStorage) Set(key string, data json.RawMessage) (string, error) {
	if key == f.tombstoneKey {
		return key, f.err
	}
	return f.Database.Set(key, data)
}

// TestDeleteTombstoneAtomicity pins the invariant that a tombstone-write
// failure must not leave the item physically deleted with no record of the
// delete. The original ordering (db.Del → db.Set tombstone) was vulnerable:
// if the tombstone Set failed (or the process was cancelled between the two
// writes), the item was gone but no tombstone existed, so a subsequent sync
// round would re-fetch from a node that hadn't observed the delete and the
// item would silently resurrect.
//
// The fix writes the tombstone first; on Set error we bail before the Del.
// "Item gone + no tombstone" is unreachable via this handler. An orphan
// tombstone (Del fails after tombstone written) is recoverable by sync —
// the item gets deleted everywhere via the recorded ts — so it is not the
// state we guard against here.
func TestDeleteTombstoneAtomicity(t *testing.T) {
	monotonic.Init()
	real := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, real.Start(storage.Options{}))
	defer real.Close()
	storage.WatchWithCallback(real, func(storage.Event) {})

	itemKey := "things/abc"
	tombstoneKey := StoragePrefix + "things"

	// Seed the item so a successful Del would actually remove something.
	nowUnix := time.Now().UTC().UnixNano()
	obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: "abc", Path: itemKey, Data: []byte(`{"v":1}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = real.SetWithMeta(itemKey, body, nowUnix, nowUnix)
	require.NoError(t, err)

	// Confirm seed.
	_, err = real.Get(itemKey)
	require.NoError(t, err, "seed precondition: item must be present before Delete handler runs")

	failing := &failingTombstoneStorage{
		Database:     real,
		tombstoneKey: tombstoneKey,
		err:          errors.New("simulated tombstone write rejection"),
	}

	handler := Delete(failing, "things", nil, nil)

	deleteTS := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/abc/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "abc", "time": deleteTS})
	w := httptest.NewRecorder()

	handler(w, req)

	_, getErr := real.Get(itemKey)
	itemPresent := getErr == nil

	tombObj, tombErr := real.Get(tombstoneKey)
	tombstonePresent := tombErr == nil && len(tombObj.Data) > 0 && string(tombObj.Data) != "null"

	if !itemPresent && !tombstonePresent {
		t.Fatalf("delete left item gone with no tombstone — sync round will resurrect it; status=%d itemErr=%v tombErr=%v",
			w.Code, getErr, tombErr)
	}
	// Bonus: when the tombstone write fails we should also surface that to the
	// client rather than reporting success.
	require.NotEqual(t, 200, w.Code, "tombstone write failed but handler returned 200; client cannot retry an error it never saw")
}

// TestDeleteHappyPathStillCommitsBoth guards against an over-eager fix that
// returns early in the success case. With healthy storage the handler must
// still remove the item and write the tombstone.
func TestDeleteHappyPathStillCommitsBoth(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	itemKey := "things/abc"
	tombstoneKey := StoragePrefix + "things"

	nowUnix := time.Now().UTC().UnixNano()
	obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: "abc", Path: itemKey, Data: []byte(`{"v":1}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = db.SetWithMeta(itemKey, body, nowUnix, nowUnix)
	require.NoError(t, err)

	handler := Delete(db, "things", nil, nil)
	deleteTS := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/abc/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "abc", "time": deleteTS})
	w := httptest.NewRecorder()

	handler(w, req)

	require.Equal(t, 200, w.Code)
	_, getErr := db.Get(itemKey)
	require.Error(t, getErr, "item must be removed on the happy path")

	tomb, err := db.Get(tombstoneKey)
	require.NoError(t, err, "tombstone must be present on the happy path")
	require.Equal(t, deleteTS, strings.TrimSpace(string(tomb.Data)), "tombstone payload must be the delete timestamp")
}
