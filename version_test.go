package pivot

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionHandler(t *testing.T) {
	handler := VersionHandler()
	req := httptest.NewRequest("GET", "/_pivot/version", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	require.Equal(t, 200, w.Code)

	var info VersionInfo
	err := json.NewDecoder(w.Body).Decode(&info)
	require.NoError(t, err)
	require.Equal(t, ProtocolVersion, info.Protocol)
	require.Equal(t, "github.com/benitogf/pivot", info.Package)
}
