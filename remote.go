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

// ClientOpts holds HTTP client configuration for remote operations.
type ClientOpts struct {
	Client *http.Client
	Leader string // Leader/pivot server address
	SSL    bool   // Use HTTPS instead of HTTP
}

// Scheme returns "https" if SSL is true, "http" otherwise.
func (c ClientOpts) Scheme() string {
	if c.SSL {
		return "https"
	}
	return "http"
}

// URL constructs a full URL with the appropriate scheme.
func (c ClientOpts) URL(path string) string {
	return c.Scheme() + "://" + c.Leader + path
}

// OriginatorHeader is the HTTP header used to identify the node that originated a change
const OriginatorHeader = "X-Pivot-Originator"

// TriggerNodeSync will call pivot on a node server
func TriggerNodeSync(client *http.Client, node string) {
	TriggerNodeSyncWithHealth(ClientOpts{Client: client}, node, "")
}

// TriggerNodeSyncWithHealth triggers a pull-only sync on a node server.
// Uses /synchronize/pivot endpoint so node pulls from pivot without sending data back.
// This prevents re-adding items that pivot just deleted.
// Uses the client's timeout (or 500ms default) for quick failure detection.
// If keyPath is provided, only that specific key will be synced.
func TriggerNodeSyncWithHealth(opts ClientOpts, node string, keyPath string) bool {
	timeout := 500 * time.Millisecond
	if opts.Client != nil && opts.Client.Timeout > 0 {
		timeout = opts.Client.Timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	url := opts.Scheme() + "://" + node + RoutePrefix + "/synchronize/pivot"
	if keyPath != "" {
		url += "?key=" + keyPath
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}

	resp, err := opts.Client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

func getEntriesFromLeader(opts ClientOpts, key string) ([]meta.Object, error) {
	var objs []meta.Object
	resp, err := opts.Client.Get(opts.URL(RoutePrefix + "/pivot/" + key))
	if err != nil {
		return objs, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return objs, errors.New("failed to get " + key + " from leader " + resp.Status)
	}

	objs, err = meta.DecodeListFromReader(resp.Body)
	if err != nil {
		return objs, err
	}

	return objs, nil
}

func getEntryFromLeader(opts ClientOpts, key string) (meta.Object, error) {
	var obj meta.Object
	resp, err := opts.Client.Get(opts.URL(RoutePrefix + "/pivot/" + key))
	if err != nil {
		return obj, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return obj, errors.New("failed to get " + key + " from leader " + resp.Status)
	}

	return meta.DecodeFromReader(resp.Body)
}

func sendToLeader(opts ClientOpts, key string, obj meta.Object, originator string) error {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(obj)
	req, err := http.NewRequest("POST", opts.URL(RoutePrefix+"/pivot/"+key), buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("failed to send update to leader " + resp.Status)
	}

	return nil
}

func sendDeleteToLeader(opts ClientOpts, key string, lastEntry int64, originator string) error {
	url := opts.URL(RoutePrefix + "/pivot/" + key + "/" + strconv.FormatInt(lastEntry, 10))
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("failed to send delete to leader " + resp.Status)
	}

	return nil
}
