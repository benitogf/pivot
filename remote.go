package pivot

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/benitogf/ooo/meta"
)

// TriggerNodeSync will call pivot on a node server
func TriggerNodeSync(client *http.Client, node string) {
	TriggerNodeSyncWithHealth(client, node)
}

// TriggerNodeSyncWithHealth triggers a pull-only sync on a node server.
// Uses /synchronize/pivot endpoint so node pulls from pivot without sending data back.
// This prevents re-adding items that pivot just deleted.
func TriggerNodeSyncWithHealth(client *http.Client, node string) bool {
	resp, err := client.Get("http://" + node + RoutePrefix + "/synchronize/pivot")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}
	return true
}

func getEntriesFromPivot(client *http.Client, pivot string, key string) ([]meta.Object, error) {
	var objs []meta.Object
	resp, err := client.Get("http://" + pivot + RoutePrefix + "/pivot/" + key)
	if err != nil {
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

func getEntryFromPivot(client *http.Client, pivot string, key string) (meta.Object, error) {
	var obj meta.Object
	resp, err := client.Get("http://" + pivot + RoutePrefix + "/pivot/" + key)
	if err != nil {
		return obj, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return obj, errors.New("failed to get " + key + " from pivot " + resp.Status)
	}

	return meta.DecodeFromReader(resp.Body)
}

func sendToPivot(client *http.Client, key string, pivot string, obj meta.Object) error {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(obj)
	resp, err := client.Post("http://"+pivot+RoutePrefix+"/pivot/"+key, "application/json", buf)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("failed to send update to pivot " + resp.Status)
	}

	return nil
}

func sendDelete(client *http.Client, key, pivot string, lastEntry int64) error {
	req, err := http.NewRequest("DELETE", "http://"+pivot+RoutePrefix+"/pivot/"+key+"/"+strconv.FormatInt(lastEntry, 10), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("failed to send delete to pivot " + resp.Status)
	}

	return nil
}
