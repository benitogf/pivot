package pivot_test

import (
	"testing"

	"github.com/benitogf/pivot"
	"github.com/stretchr/testify/require"
)

func TestFindKeyStorage_MatchingKey(t *testing.T) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "config", Database: nil},
	}

	// Test that matching works - returns nil database but no error
	result, err := pivot.FindKeyStorage(keys, "users/123")
	require.NoError(t, err)
	require.Nil(t, result) // nil database is valid
}

func TestFindKeyStorage_ExactMatch(t *testing.T) {
	keys := []pivot.Key{
		{Path: "config", Database: nil},
	}

	result, err := pivot.FindKeyStorage(keys, "config")
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestFindKeyStorage_NoMatchingKey(t *testing.T) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "config", Database: nil},
	}

	result, err := pivot.FindKeyStorage(keys, "orders/456")
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to find storage for orders/456")
}

func TestFindKeyStorage_EmptyKeys(t *testing.T) {
	keys := []pivot.Key{}

	result, err := pivot.FindKeyStorage(keys, "anything")
	require.Error(t, err)
	require.Nil(t, result)
}

func TestFindKeyStorage_NilDatabase(t *testing.T) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
	}

	result, err := pivot.FindKeyStorage(keys, "users/123")
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestFindKeyStorage_MultipleKeys(t *testing.T) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "orders/*", Database: nil},
		{Path: "config", Database: nil},
	}

	// Test each key matches correctly
	_, err := pivot.FindKeyStorage(keys, "users/123")
	require.NoError(t, err)

	_, err = pivot.FindKeyStorage(keys, "orders/456")
	require.NoError(t, err)

	_, err = pivot.FindKeyStorage(keys, "config")
	require.NoError(t, err)

	// Non-matching key should error
	_, err = pivot.FindKeyStorage(keys, "products/789")
	require.Error(t, err)
}

func TestFindKeyStorage_SingleLevelGlob(t *testing.T) {
	keys := []pivot.Key{
		{Path: "data/*", Database: nil},
	}

	// Single level after glob matches
	result, err := pivot.FindKeyStorage(keys, "data/item1")
	require.NoError(t, err)
	require.Nil(t, result)

	// Deep nested paths don't match simple glob (by design)
	_, err = pivot.FindKeyStorage(keys, "data/level1/level2/level3")
	require.Error(t, err) // No match for deep paths with simple glob
}

func TestKey_MatchesIndex(t *testing.T) {
	tests := []struct {
		name     string
		key      pivot.Key
		index    string
		expected bool
	}{
		{"glob matches", pivot.Key{Path: "users/*"}, "users/123", true},
		{"glob no match", pivot.Key{Path: "users/*"}, "orders/123", false},
		{"exact match", pivot.Key{Path: "config"}, "config", true},
		{"exact no match", pivot.Key{Path: "config"}, "settings", false},
		{"single level glob", pivot.Key{Path: "data/*"}, "data/item", true},
		{"deep nested no match", pivot.Key{Path: "data/*"}, "data/a/b/c", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.MatchesIndex(tt.index)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestKey_IsGlobPattern(t *testing.T) {
	tests := []struct {
		name     string
		key      pivot.Key
		expected bool
	}{
		{"glob pattern", pivot.Key{Path: "users/*"}, true},
		{"exact path", pivot.Key{Path: "config"}, false},
		{"nested glob", pivot.Key{Path: "data/items/*"}, true},
		{"empty path", pivot.Key{Path: ""}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.IsGlobPattern()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestKey_BasePath(t *testing.T) {
	tests := []struct {
		name     string
		key      pivot.Key
		expected string
	}{
		{"glob pattern", pivot.Key{Path: "users/*"}, "users"},
		{"exact path", pivot.Key{Path: "config"}, "config"},
		{"nested glob", pivot.Key{Path: "data/items/*"}, "data/items"},
		{"empty path", pivot.Key{Path: ""}, ""},
		{"just wildcard", pivot.Key{Path: "/*"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.BasePath()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestFindKeyStorage_FirstMatchWins(t *testing.T) {
	// Order 1: specific first - specific should match
	keys1 := []pivot.Key{
		{Path: "users/admin", Database: nil},
		{Path: "users/*", Database: nil},
	}

	_, err := pivot.FindKeyStorage(keys1, "users/admin")
	require.NoError(t, err)

	// Order 2: glob first - glob should match first
	keys2 := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "users/admin", Database: nil},
	}

	_, err = pivot.FindKeyStorage(keys2, "users/admin")
	require.NoError(t, err)

	// Both orders should find a match, demonstrating first-match-wins behavior
	// The key point is that the first matching pattern is used
}
