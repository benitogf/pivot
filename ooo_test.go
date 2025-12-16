package pivot_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	"github.com/benitogf/ooo/messages"
	"github.com/benitogf/ooo/meta"
	"github.com/gorilla/websocket"
)

// Get performs an authenticated HTTP GET and returns the parsed object
func Get[T any](server *ooo.Server, path string, authHeader http.Header) (client.Meta[T], error) {
	var result client.Meta[T]
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return result, err
	}
	for k, v := range authHeader {
		req.Header[k] = v
	}
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	response := w.Result()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return result, err
	}
	var object meta.Object
	err = json.Unmarshal(body, &object)
	if err != nil {
		return result, err
	}
	result.Created = object.Created
	result.Updated = object.Updated
	result.Index = object.Index
	err = json.Unmarshal(object.Data, &result.Data)
	return result, err
}

// Push performs an authenticated HTTP POST to create a new item and returns the index
func Push[T any](server *ooo.Server, path string, item T, authHeader http.Header) (string, error) {
	payload, err := json.Marshal(item)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest("POST", path, bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	for k, v := range authHeader {
		req.Header[k] = v
	}
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	response := w.Result()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	var object meta.Object
	err = json.Unmarshal(body, &object)
	if err != nil {
		return "", err
	}
	return object.Index, nil
}

// Set performs an authenticated HTTP POST to update an existing item
func Set[T any](server *ooo.Server, path string, item T, authHeader http.Header) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", path, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	for k, v := range authHeader {
		req.Header[k] = v
	}
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	return nil
}

// subscribeWithAuth subscribes to a websocket path with auth headers and calls callback on each message
// Same as ooo/client.Subscribe but with custom header support
func subscribeWithAuth[T any](ctx context.Context, host, path string, authHeader http.Header, callback func([]client.Meta[T])) {
	wsURLStr := "ws://" + host + path

	wsClient, _, err := websocket.DefaultDialer.Dial(wsURLStr, authHeader)
	if err != nil {
		return
	}

	go func() {
		<-ctx.Done()
		wsClient.Close()
	}()

	var cache json.RawMessage
	isList := path[len(path)-1] == '*'

	for {
		_, message, err := wsClient.ReadMessage()
		if err != nil {
			return
		}

		result := []client.Meta[T]{}
		if isList {
			var objs []meta.Object
			cache, objs, err = messages.PatchList(message, cache)
			if err != nil {
				return
			}
			for _, obj := range objs {
				var item T
				err = json.Unmarshal(obj.Data, &item)
				if err != nil {
					continue
				}
				result = append(result, client.Meta[T]{
					Created: obj.Created,
					Updated: obj.Updated,
					Index:   obj.Index,
					Data:    item,
				})
			}
		} else {
			var obj meta.Object
			cache, obj, err = messages.Patch(message, cache)
			if err != nil {
				return
			}
			var item T
			err = json.Unmarshal(obj.Data, &item)
			if err != nil {
				return
			}
			result = append(result, client.Meta[T]{
				Created: obj.Created,
				Updated: obj.Updated,
				Index:   obj.Index,
				Data:    item,
			})
		}
		callback(result)
	}
}
