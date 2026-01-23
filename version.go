package pivot

import (
	"encoding/json"
	"net/http"
)

const ProtocolVersion = "2.0"

type VersionInfo struct {
	Protocol string `json:"protocol"`
	Package  string `json:"package"`
}

func VersionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(VersionInfo{
			Protocol: ProtocolVersion,
			Package:  "github.com/benitogf/pivot",
		})
	}
}
