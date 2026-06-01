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

// ActivityHeader is the response header used by the leader's GetList endpoint to
// piggyback the activity timestamp (matching /activity's LastEntry), so a node
// pushing changes up doesn't need a second HTTP round-trip just to learn it.
// Old leaders don't emit it; new clients fall back to checkLeaderActivity.
const ActivityHeader = "X-Pivot-Activity"

// VVHeader carries the originator's local version vector for a key on
// inbound Set/Delete. Receivers parse it and merge into local VV so
// peer counters integrate (the merge-on-receive half of the VV
// foundation). Old peers don't emit it; missing/empty header → no
// merge, falls through to existing behavior.
//
// Trust boundary: the receiver merges via element-wise max into its
// own VV state. A peer sending a counter near math.MaxInt64 would
// permanently advance the receiver's view of that node and the real
// value could never catch up. Closed-network deployment (per the
// repo's review preamble) is the trust boundary that makes this
// acceptable; the wire is authoritative on local counters and any
// future open-network use needs an out-of-band signed VV or a
// separate authority on counter advancement.
const VVHeader = "X-Pivot-VV"

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

// getEntriesAndActivityFromLeader fetches list entries and, if the leader emits
// the X-Pivot-Activity header, the activity timestamp in the same round-trip.
// hasActivity is false against older leaders that don't emit the header — the
// caller is expected to fall back to checkLeaderActivity in that case.
func getEntriesAndActivityFromLeader(opts ClientOpts, key string) (objs []meta.Object, activity int64, hasActivity bool, err error) {
	resp, err := opts.Client.Get(opts.URL(RoutePrefix + "/pivot/" + key))
	if err != nil {
		return nil, 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, 0, false, errors.New("failed to get " + key + " from leader " + resp.Status)
	}

	if v := resp.Header.Get(ActivityHeader); v != "" {
		if parsed, perr := strconv.ParseInt(v, 10, 64); perr == nil {
			activity = parsed
			hasActivity = true
		}
	}

	objs, err = meta.DecodeListFromReader(resp.Body)
	if err != nil {
		return nil, activity, hasActivity, err
	}

	return objs, activity, hasActivity, nil
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

// sendToLeader pushes a single item to the leader. On a 200 response,
// the leader's post-write VersionVector is returned via the second
// value (decoded from the VVHeader on the response). Empty when the
// leader is an older peer that doesn't echo VV — callers should treat
// that as the legacy "no info" case.
func sendToLeader(opts ClientOpts, key string, obj meta.Object, originator string, vv VersionVector) (VersionVector, error) {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(obj)
	req, err := http.NewRequest("POST", opts.URL(RoutePrefix+"/pivot/"+key), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
	}
	if len(vv) > 0 {
		req.Header.Set(VVHeader, string(encodeVV(vv)))
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("failed to send update to leader " + resp.Status)
	}

	leaderVV, _ := decodeVVHeader(resp.Header.Get(VVHeader))
	return leaderVV, nil
}

// sendDeleteToLeader mirrors sendToLeader for delete operations. The
// returned VV (when non-empty) is pivot's post-delete VV; senders merge
// it locally so their VVManager reflects pivot's frontier.
func sendDeleteToLeader(opts ClientOpts, key string, lastEntry int64, originator string, vv VersionVector) (VersionVector, error) {
	url := opts.URL(RoutePrefix + "/pivot/" + key + "/" + strconv.FormatInt(lastEntry, 10))
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	if originator != "" {
		req.Header.Set(OriginatorHeader, originator)
	}
	if len(vv) > 0 {
		req.Header.Set(VVHeader, string(encodeVV(vv)))
	}
	resp, err := opts.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("failed to send delete to leader " + resp.Status)
	}

	leaderVV, _ := decodeVVHeader(resp.Header.Get(VVHeader))
	return leaderVV, nil
}
