package pivot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/benitogf/ooo/meta"
)

// TriggerNodeSync will call pivot on a node server
func TriggerNodeSync(client *http.Client, node string) {
	TriggerNodeSyncWithHealth(client, node)
}

// TriggerNodeSyncWithHealth triggers a pull-only sync on a node server.
// Uses /synchronize/pivot endpoint so node pulls from pivot without sending data back.
// This prevents re-adding items that pivot just deleted.
// Uses a 5 second timeout to allow sync to complete on slow networks.
func TriggerNodeSyncWithHealth(client *http.Client, node string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+node+RoutePrefix+"/synchronize/pivot", nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
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

// OriginatorHeader is the HTTP header used to identify the node that originated a change
const OriginatorHeader = "X-Pivot-Originator"

func sendToPivot(client *http.Client, key string, pivot string, obj meta.Object, originator string) error {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(obj)
	req, err := http.NewRequest("POST", "http://"+pivot+RoutePrefix+"/pivot/"+key, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("failed to send update to pivot " + resp.Status)
	}

	return nil
}

func sendDelete(client *http.Client, key, pivot string, lastEntry int64, originator string) error {
	url := "http://" + pivot + RoutePrefix + "/pivot/" + key + "/" + strconv.FormatInt(lastEntry, 10)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
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
