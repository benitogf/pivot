package pivot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/gorilla/mux"
)

var Mu sync.Mutex

// ActivityEntry keeps the time of the last entry
type ActivityEntry struct {
	LastEntry int64 `json:"lastEntry"`
}

// GetNodes function that returns the nodes ips
type GetNodes func() []string

type Key struct {
	Path     string
	Database ooo.Database
}

func FindStorageFor(keys []Key, index string) (ooo.Database, error) {
	for _, _key := range keys {
		if key.Match(_key.Path, index) {
			return _key.Database, nil
		}
	}
	return nil, errors.New("failed to find storage for " + index)
}

func lastActivity(objs []meta.Object) int64 {
	if len(objs) > 0 {
		return max(objs[0].Created, objs[0].Updated)
	}

	return 0
}

func checkLastDelete(storage ooo.Database, lastEntry int64, key string) int64 {
	lastDelete, err := storage.Get("pivot:" + key)
	if err != nil {
		// log.Println("failed to get last delete of ", key, err)
		return lastEntry
	}

	obj, err := meta.Decode(lastDelete)
	if err != nil {
		// log.Println("failed to decode object of last delete for ", key, lastDelete, err)
		return lastEntry
	}

	lastDeleteNum, err := strconv.Atoi(string(obj.Data))
	if err != nil {
		// log.Println("failed to decode last delete of ", key, lastDelete, err)
		return lastEntry
	}

	return max(lastEntry, int64(lastDeleteNum))
}

func checkActivity(_key Key) (ActivityEntry, error) {
	var activity ActivityEntry
	entries, err := _key.Database.Get(_key.Path)
	if err != nil {
		// log.Println("failed to fetch local "+_key, err)
		return activity, nil
	}
	baseKey := _key

	if key.LastIndex(_key.Path) == "*" {
		_baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objs, err := meta.DecodeList(entries)
		if err != nil {
			// log.Println("failed to decode "+_key+" objects list", err)
			return activity, err
		}

		activity.LastEntry = checkLastDelete(_key.Database, lastActivity(objs), _baseKey)
		return activity, nil
	}

	obj, err := meta.Decode(entries)
	if err != nil {
		// log.Println("failed to decode "+_key+" objects list", err)
		return activity, err
	}

	activity.LastEntry = checkLastDelete(_key.Database, max(obj.Created, obj.Updated), baseKey.Path)
	return activity, nil
}

func checkPivotActivity(client *http.Client, pivot string, key string) (ActivityEntry, error) {
	var activity ActivityEntry
	resp, err := client.Get("http://" + pivot + "/activity/" + key)
	if err != nil {
		// log.Println("failed to get activity on "+key+" from pivot at "+pivot, err)
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

func getEntriesFromPivot(client *http.Client, pivot string, key string) ([]meta.Object, error) {
	var objs []meta.Object
	resp, err := client.Get("http://" + pivot + "/" + key)
	if err != nil {
		// log.Println("failed to get "+key+" from pivot", err)
		return objs, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return objs, errors.New("failed to get " + key + " from pivot " + resp.Status)
	}

	objs, err = meta.DecodeListFromReader(resp.Body)
	if err != nil {
		return objs, err
	}

	return objs, nil
}

// TriggerNodeSync will call pivot on a node server
func TriggerNodeSync(client *http.Client, node string) {
	// log.Println("node sync", node)
	resp, err := client.Get("http://" + node + "/pivot")
	if err != nil {
		// log.Println("failed to trigger sync from pivot on ", node, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// log.Println("failed to trigger sync from pivot on " + node + " " + resp.Status)
		return
	}
}

func getEntryFromPivot(client *http.Client, pivot string, key string) (meta.Object, error) {
	var obj meta.Object
	resp, err := client.Get("http://" + pivot + "/" + key)
	if err != nil {
		// log.Println("failed to get "+key+" from pivot", err)
		return obj, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return obj, errors.New("failed to get " + key + " from pivot " + resp.Status)
	}

	return meta.DecodeFromReader(resp.Body)
}

// get from pivot and write to local
func syncLocalEntries(client *http.Client, pivot string, _key Key, lastEntry int64) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			// log.Println("sync local " + baseKey + " failed to get from pivot")
			return err
		}

		localData, err := _key.Database.Get(_key.Path)
		if err != nil {
			// log.Println("sync local " + _key.Path + " failed to read local entries")
			return err
		}

		objsLocal, err := meta.DecodeList(localData)
		if err != nil {
			// log.Println("sync local " + _key + " failed to decode local entries")
			return err
		}

		objsToDelete := getEntriesNegativeDiff(objsLocal, objsPivot)
		for _, index := range objsToDelete {
			_key.Database.Del(baseKey + "/" + index)
		}

		objsToSend := getEntriesPositiveDiff(objsLocal, objsPivot)
		for _, obj := range objsToSend {
			_key.Database.SetWithMeta(baseKey+"/"+obj.Index, obj.Data, obj.Created, obj.Updated)
			// if err != nil {
			// 	log.Println("failed to store entry from pivot", err)
			// }
		}
		_key.Database.Set("pivot:"+baseKey, json.RawMessage(strconv.FormatInt(lastEntry, 10)))
		return nil
	}

	obj, err := getEntryFromPivot(client, pivot, _key.Path)
	if err != nil {
		// log.Println("sync local " + _key.Path + " failed to get from pivot")
		return err
	}
	_key.Database.SetWithMeta(_key.Path, obj.Data, obj.Created, obj.Updated)
	// if err != nil {
	// 	log.Println("failed to store entry from pivot", err)
	// }
	_key.Database.Set("pivot:"+_key.Path, json.RawMessage(strconv.FormatInt(lastEntry, 10)))

	return nil
}

func sendToPivot(client *http.Client, key string, pivot string, obj meta.Object) error {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(obj)
	resp, err := client.Post("http://"+pivot+"/pivot/"+key, "application/json", buf)
	if err != nil {
		// log.Println("failed to send update to pivot", err)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// log.Println("http://" + pivot + "/pivot/" + key)
		// log.Println("failed to send update to pivot " + resp.Status)
		return errors.New("failed to send update to pivot " + resp.Status)
	}

	return nil
}

func sendDelete(client *http.Client, key, pivot string, lastEntry int64) error {
	req, err := http.NewRequest("DELETE", "http://"+pivot+"/pivot/"+key+"/"+strconv.FormatInt(lastEntry, 10), nil)
	if err != nil {
		// log.Println("failed to send delete to pivot", err)
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		// log.Println("failed to send delete to pivot", err)
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		// log.Println("http://" + pivot + "/pivot/" + key)
		// log.Println("failed to send delete to pivot " + resp.Status)
		return errors.New("failed to send delete to pivot " + resp.Status)
	}

	return nil
}

func getEntriesNegativeDiff(objsDst, objsSrc []meta.Object) []string {
	var result []string
	for _, objDst := range objsDst {
		found := false
		for _, objSrc := range objsSrc {
			if objSrc.Index == objDst.Index {
				found = true
				break
			}
		}
		if !found {
			result = append(result, objDst.Index)
		}
	}
	return result
}

func getEntriesPositiveDiff(objsDst, objsSrc []meta.Object) []meta.Object {
	var result []meta.Object
	for _, objSrc := range objsSrc {
		needsUpdate := false
		found := false
		for _, objDst := range objsDst {
			if objSrc.Index == objDst.Index {
				found = true
			}
			if objSrc.Index == objDst.Index && objSrc.Updated > objDst.Updated {
				needsUpdate = true
				break
			}
		}
		if needsUpdate || !found {
			result = append(result, objSrc)
		}
	}
	return result
}

// get from local and send to pivot (updates only, no new entries or deletes)
func syncPivotEntries(client *http.Client, pivot string, _key Key, lastEntry int64) error {
	localData, err := _key.Database.Get(_key.Path)
	if err != nil {
		// log.Println("sync pivot " + _key + " failed to read local entries")
		return err
	}
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsLocal, err := meta.DecodeList(localData)
		if err != nil {
			// log.Println("sync pivot " + _key + " failed to decode local entries")
			return err
		}

		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			// log.Println("sync pivot " + baseKey + " failed to get from pivot")
			return err
		}

		objsToDelete := getEntriesNegativeDiff(objsPivot, objsLocal)
		for _, index := range objsToDelete {
			sendDelete(client, baseKey+"/"+index, pivot, lastEntry)
		}

		objsToSend := getEntriesPositiveDiff(objsPivot, objsLocal)
		for _, obj := range objsToSend {
			sendToPivot(client, baseKey+"/"+obj.Index, pivot, obj)
		}

		return nil
	}

	obj, err := meta.Decode(localData)
	if err != nil {
		// log.Println("sync pivot " + _key + " failed to decode local entries")
		return err
	}
	sendToPivot(client, obj.Index, pivot, obj)

	return nil
}

func synchronizeItem(client *http.Client, pivot string, key Key) error {
	update := false
	_key := strings.Replace(key.Path, "/*", "", 1)
	//check
	activityPivot, err := checkPivotActivity(client, pivot, _key)
	if err != nil {
		// log.Println(err)
		return errors.New("failed to check activity for " + _key + " on pivot")
	}
	activityLocal, err := checkActivity(key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on local")
	}

	// sync
	if activityLocal.LastEntry > activityPivot.LastEntry {
		err := syncPivotEntries(client, pivot, key, activityLocal.LastEntry)
		if err != nil {
			return err
		}
		update = true
	}

	if activityLocal.LastEntry < activityPivot.LastEntry {
		err := syncLocalEntries(client, pivot, key, activityPivot.LastEntry)
		if err != nil {
			return err
		}
		update = true
	}

	if update {
		return nil
	}

	return errors.New("nothing to synchronize for " + key.Path)
}

// Synchronize a list of keys
func Synchronize(client *http.Client, pivot string, keys []Key) error {
	// prevent simultaneous sync operations - use TryLock to avoid deadlock
	// when BeforeRead triggers Synchronize during an ongoing sync
	if !Mu.TryLock() {
		return nil
	}
	defer Mu.Unlock()

	update := false
	for _, key := range keys {
		errItem := synchronizeItem(client, pivot, key)
		if errItem == nil {
			update = true
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// SyncReadFilter filter to synchronize with pivot on read
func SyncReadFilter(client *http.Client, pivot string, keys []Key) ooo.Apply {
	return func(index string, data json.RawMessage) (json.RawMessage, error) {
		if pivot != "" {
			err := Synchronize(client, pivot, keys)
			if err != nil {
				return data, nil
			}

			storage, err := FindStorageFor(keys, index)
			if err != nil {
				return data, nil
			}

			updatedData, err := storage.Get(index)
			if err != nil {
				return data, nil
			}

			return updatedData, nil
		}

		return data, nil
	}
}

// SyncWriteFilter filter to synchronize nodes on write
func SyncWriteFilter(client *http.Client, pivotIP string, getNodes GetNodes) ooo.Notify {
	return func(index string) {
		// log.Println("sync write", index)
		if pivotIP == "" {
			for _, node := range getNodes() {
				TriggerNodeSync(client, node)
			}
		}
	}
}

// SyncDeleteFilter update the last delete time on each delete
func SyncDeleteFilter(client *http.Client, pivotIP string, storage ooo.Database, key string, getNodes GetNodes) ooo.ApplyDelete {
	return func(index string) error {
		if pivotIP == "" {
			for _, node := range getNodes() {
				TriggerNodeSync(client, node)
			}
		}

		storage.Set("pivot:"+key, json.RawMessage(ooo.Time()))
		return nil
	}
}

// StorageSyncCallback is the callback type for storage sync events
type StorageSyncCallback func(event ooo.StorageEvent)

// StorageSync creates a StorageSyncCallback that triggers synchronization on storage events.
// This replaces the need for SyncWriteFilter and SyncDeleteFilter.
// For pivot servers (pivotIP == ""), it notifies all nodes on any storage change.
// For node servers, it synchronizes with the pivot on any storage change.
func StorageSync(client *http.Client, pivotIP string, keys []Key, getNodes GetNodes) StorageSyncCallback {
	return func(event ooo.StorageEvent) {
		// Find matching key and its database for this event
		var matchedKey string
		var matchedDB ooo.Database
		for _, k := range keys {
			if key.Match(k.Path, event.Key) {
				matchedKey = strings.Replace(k.Path, "/*", "", 1)
				matchedDB = k.Database
				break
			}
		}

		if matchedKey == "" || matchedDB == nil {
			return
		}

		// Track delete timestamps for proper sync
		if event.Operation == "del" {
			matchedDB.Set("pivot:"+matchedKey, json.RawMessage(ooo.Time()))
		}

		if pivotIP == "" {
			// This is the pivot server - notify all nodes synchronously
			// so that data is available on nodes before returning
			for _, node := range getNodes() {
				TriggerNodeSync(client, node)
			}
		} else {
			// This is a node - synchronize with pivot
			go Synchronize(client, pivotIP, keys)
		}
	}
}

// WatchStorage watches a storage's events and calls the callback for each event.
// Use this to watch additional storages beyond the main server.Storage.
// This replaces ooo.WatchStorageNoop for storages that need sync.
func WatchStorage(storage ooo.Database, callback StorageSyncCallback) {
	for {
		event := <-storage.Watch()
		if !storage.Active() {
			break
		}
		if event.Key != "" {
			callback(event)
		}
	}
}

// Pivot endpoint to trigger a synchronize from the pivot server
func Pivot(client *http.Client, pivot string, keys []Key) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if pivot == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "this method should not be called on the pivot server")
			return
		}

		Synchronize(client, pivot, keys)
		w.WriteHeader(http.StatusOK)
	}
}

// Get WILL
func Get(storage ooo.Database, key string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var objs []meta.Object
		raw, err := storage.Get(key)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		err = json.Unmarshal(raw, &objs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(objs)
	}
}

// Set set data on the pivot instance
func Set(storage ooo.Database, key string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoded, err := meta.DecodeFromReader(r.Body)
		if err != nil {
			// log.Println("failed to decode "+key+" entry on pivot", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		index := mux.Vars(r)["index"]
		itemKey := key + "/" + decoded.Index
		if index == "" {
			itemKey = key
		}
		_, err = storage.SetWithMeta(itemKey, decoded.Data, decoded.Created, decoded.Updated)
		if err != nil {
			// log.Println("failed to store on pivot "+key+" entry", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Delete delete data on the pivot instance
func Delete(storage ooo.Database, key string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		index := mux.Vars(r)["index"]
		time := mux.Vars(r)["time"]
		itemKey := key + "/" + index
		err := storage.Del(itemKey)
		storage.Set("pivot:"+key, json.RawMessage(time))
		if err != nil {
			// log.Println("failed to delete on pivot "+key+" entry", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Activity route to get activity info from the pivot instance
func Activity(key Key) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if key.Database == nil || !key.Database.Active() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		activity, _ := checkActivity(key)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(activity)
	}
}

// Setup configures pivot synchronization for a server.
// It sets up OnStorageEvent for write/delete sync on server.Storage,
// HTTP routes for the pivot protocol, and returns a BeforeRead callback
// for sync-on-read at the storage level.
// Each key must have its Database field set to the storage it uses.
// For keys using server.Storage, OnStorageEvent handles write sync automatically.
// For keys using separate storages, use SetupStorage to get callbacks for that storage.
func Setup(server *ooo.Server, keys []Key, getNodes GetNodes) func(key string) {
	pivotIP := server.Pivot
	client := server.Client

	// Set up OnStorageEvent for write/delete synchronization on server.Storage
	syncCallback := StorageSync(client, pivotIP, keys, getNodes)
	server.OnStorageEvent = ooo.StorageEventCallback(syncCallback)

	// Set up HTTP routes for pivot protocol
	server.Router.HandleFunc("/pivot", Pivot(client, pivotIP, keys)).Methods("GET")
	for _, k := range keys {
		baseKey := strings.Replace(k.Path, "/*", "", 1)
		server.Router.HandleFunc("/activity/"+baseKey, Activity(k)).Methods("GET")
		if baseKey != k.Path {
			server.Router.HandleFunc("/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}", Set(k.Database, baseKey)).Methods("POST")
			server.Router.HandleFunc("/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey)).Methods("DELETE")
		} else {
			server.Router.HandleFunc("/pivot/"+baseKey, Set(k.Database, baseKey)).Methods("POST")
			server.Router.HandleFunc("/pivot/"+baseKey+"/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey)).Methods("DELETE")
		}
		// For keys with external storage (not server.Storage), expose GET route
		// so nodes can fetch data during sync
		if k.Database != nil && k.Database != server.Storage {
			server.Router.HandleFunc("/"+k.Path, Get(k.Database, k.Path)).Methods("GET")
		}
	}

	// Return BeforeRead callback for sync-on-read at storage level
	var syncing int32 // Guard to prevent recursive sync
	return func(readKey string) {
		if pivotIP == "" {
			return
		}
		// Prevent recursive sync (Synchronize calls storage.Get which triggers BeforeRead)
		if !atomic.CompareAndSwapInt32(&syncing, 0, 1) {
			return
		}
		defer atomic.StoreInt32(&syncing, 0)

		// Check if this key matches any of our sync keys
		for _, k := range keys {
			if key.Match(k.Path, readKey) {
				Synchronize(client, pivotIP, keys)
				return
			}
		}
	}
}

// SetupStorage configures sync for a separate storage (not server.Storage).
// Returns a BeforeRead callback to be passed to StorageOpt.BeforeRead.
// Call this BEFORE starting the storage, then start the storage with the returned BeforeRead.
// After starting the storage, call the returned startWatch function to begin watching for writes.
func SetupStorage(client *http.Client, pivotIP string, keys []Key, getNodes GetNodes) (beforeRead func(key string), startWatch func(storage ooo.Database)) {
	syncCallback := StorageSync(client, pivotIP, keys, getNodes)
	var syncing int32 // Guard to prevent recursive sync

	// BeforeRead callback for sync-on-read
	beforeRead = func(readKey string) {
		if pivotIP == "" {
			return
		}
		// Prevent recursive sync (Synchronize calls storage.Get which triggers BeforeRead)
		if !atomic.CompareAndSwapInt32(&syncing, 0, 1) {
			return
		}
		defer atomic.StoreInt32(&syncing, 0)

		for _, k := range keys {
			if key.Match(k.Path, readKey) {
				Synchronize(client, pivotIP, keys)
				return
			}
		}
	}

	// Function to start watching after storage is started
	startWatch = func(storage ooo.Database) {
		go WatchStorage(storage, syncCallback)
	}

	return beforeRead, startWatch
}
