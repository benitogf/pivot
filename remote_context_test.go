package pivot

// Verifies that the node→leader HTTP calls in remote.go honor a caller-supplied
// context, so a graceful Instance.Shutdown() (which cancels that context) can
// unblock requests that are in flight against a slow or hung leader rather than
// waiting out the client timeout.
//
// Pattern: a leader handler that blocks until the test releases it, a request
// fired in a goroutine, and a cancel() that must make the call return promptly
// with a context error. No sleeps — synchronization is via channels.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo/meta"
	"github.com/stretchr/testify/require"
)

// hangingLeader returns a server whose handler signals once it has a request in
// flight (reached) and then blocks until the test closes release.
func hangingLeader(t *testing.T) (opts ClientOpts, reached <-chan struct{}, cancel context.CancelFunc) {
	t.Helper()
	reachedCh := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(reachedCh) })
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	ctx, cancelFn := context.WithCancel(context.Background())
	return ClientOpts{Client: srv.Client(), Leader: srv.Listener.Addr().String(), ctx: ctx}, reachedCh, cancelFn
}

// TestInstanceShutdownCancelsContext closes the loop between Shutdown and the
// remote calls: Shutdown must cancel the instance context that the syncer
// stamps onto ClientOpts, which is what unblocks the in-flight calls above.
func TestInstanceShutdownCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	i := &Instance{ctx: ctx, cancel: cancel}

	i.Shutdown()

	require.True(t, i.IsShutdown())
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the instance context")
	}
}

func TestGetEntryFromLeaderCancelsOnContextCancel(t *testing.T) {
	opts, reached, cancel := hangingLeader(t)

	done := make(chan error, 1)
	go func() {
		_, err := getEntryFromLeader(opts, "things/1")
		done <- err
	}()

	<-reached // request is now in flight on the (hung) leader
	cancel()  // mimic Instance.Shutdown() cancelling in-flight leader calls

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("getEntryFromLeader did not unblock on context cancel")
	}
}

func TestCheckLeaderActivityCancelsOnContextCancel(t *testing.T) {
	opts, reached, cancel := hangingLeader(t)

	done := make(chan error, 1)
	go func() {
		_, err := checkLeaderActivity(opts, "things/*")
		done <- err
	}()

	<-reached
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("checkLeaderActivity did not unblock on context cancel")
	}
}

func TestSendToLeaderCancelsOnContextCancel(t *testing.T) {
	opts, reached, cancel := hangingLeader(t)

	done := make(chan error, 1)
	go func() {
		_, err := sendToLeader(opts, "things/1", meta.Object{Created: 1}, "", nil)
		done <- err
	}()

	<-reached
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("sendToLeader did not unblock on context cancel")
	}
}
