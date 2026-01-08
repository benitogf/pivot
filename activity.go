package pivot

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
)

// ActivityEntry keeps the time of the last entry
type ActivityEntry struct {
	LastEntry int64 `json:"lastEntry"`
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

func checkActivity(_key Key) (ActivityEntry, error) {
	var activity ActivityEntry
	baseKey := _key

	if key.LastIndex(_key.Path) == "*" {
		_baseKey := strings.Replace(_key.Path, "/*", "", 1)
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

func checkPivotActivity(client *http.Client, pivot string, key string) (ActivityEntry, error) {
	var activity ActivityEntry
	resp, err := client.Get("http://" + pivot + RoutePrefix + "/activity/" + key)
	if err != nil {
		return activity, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return activity, errors.New("failed to get activity on " + key + " from pivot at " + pivot)
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&activity)

	return activity, err
}
