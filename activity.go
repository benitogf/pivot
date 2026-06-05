package pivot

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
)

// ActivityEntry keeps the time of the last entry and the version vector.
// VV (Version Vector) is the authoritative sync indicator when available.
// LastEntry is used as fallback for backward compatibility with old nodes.
type ActivityEntry struct {
	LastEntry int64         `json:"lastEntry"`
	VV        VersionVector `json:"vv,omitempty"`
}

func lastActivity(objs []meta.Object) int64 {
	var maxTime int64
	for _, obj := range objs {
		objTime := max(obj.Created, obj.Updated)
		if objTime > maxTime {
			maxTime = objTime
		}
	}
	return maxTime
}

func checkLastDelete(db storage.Database, lastEntry int64, key string) int64 {
	if db == nil || !db.Active() {
		return lastEntry
	}
	obj, err := db.Get(StoragePrefix + key)
	if err != nil {
		return lastEntry
	}

	lastDeleteNum, err := strconv.Atoi(string(obj.Data))
	if err != nil {
		return lastEntry
	}

	return max(lastEntry, int64(lastDeleteNum))
}

var ErrStorageNotActive = errors.New("storage not active")

func checkActivity(_key Key) (ActivityEntry, error) {
	var activity ActivityEntry
	if _key.Database == nil || !_key.Database.Active() {
		return activity, ErrStorageNotActive
	}
	baseKey := _key

	if key.LastIndex(_key.Path) == "*" {
		_baseKey := baseKeyFromPath(_key.Path)
		objs, err := _key.Database.GetList(_key.Path)
		if err != nil {
			return activity, nil
		}

		activity.LastEntry = checkLastDelete(_key.Database, lastActivity(objs), _baseKey)
		return activity, nil
	}

	obj, err := _key.Database.Get(_key.Path)
	if err != nil {
		// Key doesn't exist - check for delete timestamp
		activity.LastEntry = checkLastDelete(_key.Database, 0, baseKey.Path)
		return activity, nil
	}

	activity.LastEntry = checkLastDelete(_key.Database, max(obj.Created, obj.Updated), baseKey.Path)
	return activity, nil
}

// checkPivotActivity is the syncer-side activity poll. It MUST take the full
// ClientOpts (not just client + leader) so the SSL flag is forwarded — without
// it, an `ssl=true` syncer would silently downgrade activity URLs to http://.
func checkPivotActivity(opts ClientOpts, key string) (ActivityEntry, error) {
	return checkLeaderActivity(opts, key)
}

func checkLeaderActivity(opts ClientOpts, key string) (ActivityEntry, error) {
	var activity ActivityEntry
	req, err := http.NewRequestWithContext(opts.reqContext(), http.MethodGet, opts.URL(RoutePrefix+"/activity/"+key), nil)
	if err != nil {
		return activity, err
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		return activity, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return activity, errors.New("failed to get activity on " + key + " from leader at " + opts.Leader)
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&activity)

	return activity, err
}
