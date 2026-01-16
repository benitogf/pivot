package pivot

import (
	"errors"

	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/storage"
)

// Key defines a path to synchronize and its associated database.
type Key struct {
	Path       string
	Database   storage.Database // nil means server.Storage
	ClusterURL string           // if set, sync FROM this cluster (overrides Config.ClusterURL)
	Local      bool             // if true, IS cluster leader for this key (ignores ClusterURL fallback)
}

// EffectiveClusterURL returns the cluster URL for this key.
// Resolution order: Local=true returns "", Key.ClusterURL if set, otherwise fallback.
func (k Key) EffectiveClusterURL(fallback string) string {
	if k.Local {
		return ""
	}
	if k.ClusterURL != "" {
		return k.ClusterURL
	}
	return fallback
}

// IsClusterLeaderFor returns true if this server IS the cluster leader for this key.
func (k Key) IsClusterLeaderFor(configClusterURL string) bool {
	return k.EffectiveClusterURL(configClusterURL) == ""
}

// FindKeyStorage finds the storage database for a given index by matching against the key paths.
// Returns an error if no matching key is found.
func FindKeyStorage(keys []Key, index string) (storage.Database, error) {
	for _, _key := range keys {
		if key.Match(_key.Path, index) {
			return _key.Database, nil
		}
	}
	return nil, errors.New("failed to find storage for " + index)
}

// MatchesIndex returns true if the key's path matches the given index.
func (k Key) MatchesIndex(index string) bool {
	return key.Match(k.Path, index)
}

// IsGlobPattern returns true if the key's path contains a wildcard pattern.
func (k Key) IsGlobPattern() bool {
	return key.LastIndex(k.Path) == "*"
}

// BasePath returns the key's path without the wildcard suffix.
// For "users/*" returns "users", for "config" returns "config".
func (k Key) BasePath() string {
	if k.IsGlobPattern() {
		// Remove "/*" suffix
		if len(k.Path) > 2 {
			return k.Path[:len(k.Path)-2]
		}
		return ""
	}
	return k.Path
}
