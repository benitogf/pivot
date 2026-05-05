package pivot

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCheckPivotActivityForwardsSSL pins the regression where checkPivotActivity
// dropped the SSL flag and silently downgraded activity URLs to http:// even
// for syncers configured with ssl=true.
//
// We assert two things:
//   - against an HTTPS leader, the call succeeds (which it can't if the URL
//     scheme is wrong; an http:// request to an https-only listener fails).
//   - the activity body parses, proving end-to-end the request reached the
//     handler over TLS.
func TestCheckPivotActivityForwardsSSL(t *testing.T) {
	// httptest.NewTLSServer issues a self-signed cert; the request URL must
	// be https:// for the listener to accept it.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, RoutePrefix+"/activity/") {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(ActivityEntry{LastEntry: 42})
	}))
	defer srv.Close()

	// Strip scheme: ClientOpts.Leader expects host:port; SSL flag drives the scheme.
	leader := srv.URL[len("https://"):]

	// srv.Client() trusts the test server's self-signed cert. Wrap it with the
	// httptest TLS config so the SSL=true path actually succeeds.
	httpsClient := srv.Client()
	httpsClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	opts := ClientOpts{Client: httpsClient, Leader: leader, SSL: true}
	activity, err := checkPivotActivity(opts, "items")
	if err != nil {
		t.Fatalf("checkPivotActivity over TLS: %v", err)
	}
	if activity.LastEntry != 42 {
		t.Fatalf("activity.LastEntry = %d; want 42", activity.LastEntry)
	}

	// Sanity check: the same call with SSL=false against the same TLS-only
	// listener must fail — proving SSL is genuinely driving the scheme rather
	// than the test passing for unrelated reasons.
	optsHTTP := ClientOpts{Client: httpsClient, Leader: leader, SSL: false}
	if _, err := checkPivotActivity(optsHTTP, "items"); err == nil {
		t.Fatalf("expected http:// request to TLS-only listener to fail; it succeeded")
	}
}
