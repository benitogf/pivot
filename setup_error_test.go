package pivot

// SetupWithError lets a host process detect an invalid pivot key configuration
// and report it cleanly, rather than being killed by the panic that the
// convenience Setup wrapper still raises for the same misconfiguration.

import (
	"testing"

	"github.com/benitogf/ooo"
	"github.com/stretchr/testify/require"
)

func TestSetupWithErrorReturnsErrorOnLocalAndClusterURL(t *testing.T) {
	var s *ooo.Server
	var err error
	require.NotPanics(t, func() {
		s, err = SetupWithError(&ooo.Server{}, Config{
			Keys: []Key{{Path: "things/*", Local: true, ClusterURL: "http://leader:8080"}},
		})
	})
	require.Error(t, err)
	require.Nil(t, s)
}

func TestSetupWithErrorReturnsErrorOnKeyClusterURLWithoutConfigClusterURL(t *testing.T) {
	var s *ooo.Server
	var err error
	require.NotPanics(t, func() {
		// Config.ClusterURL empty (pivot role) but a key points at another pivot.
		s, err = SetupWithError(&ooo.Server{}, Config{
			Keys: []Key{{Path: "things/*", ClusterURL: "http://other:8080"}},
		})
	})
	require.Error(t, err)
	require.Nil(t, s)
}

// Setup keeps its panic-on-misconfiguration contract so existing callers are
// unaffected; SetupWithError is the opt-in recoverable path.
func TestSetupStillPanicsOnMisconfiguration(t *testing.T) {
	require.Panics(t, func() {
		Setup(&ooo.Server{}, Config{
			Keys: []Key{{Path: "things/*", Local: true, ClusterURL: "http://leader:8080"}},
		})
	})
}
