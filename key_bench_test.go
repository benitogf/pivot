package pivot_test

import (
	"fmt"
	"testing"

	"github.com/benitogf/pivot"
)

func BenchmarkFindKeyStorage_SingleKey(b *testing.B) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "users/123")
	}
}

func BenchmarkFindKeyStorage_FiveKeys_FirstMatch(b *testing.B) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "orders/*", Database: nil},
		{Path: "products/*", Database: nil},
		{Path: "config", Database: nil},
		{Path: "settings", Database: nil},
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "users/123")
	}
}

func BenchmarkFindKeyStorage_FiveKeys_LastMatch(b *testing.B) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "orders/*", Database: nil},
		{Path: "products/*", Database: nil},
		{Path: "config", Database: nil},
		{Path: "settings", Database: nil},
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "settings")
	}
}

func BenchmarkFindKeyStorage_FiveKeys_NoMatch(b *testing.B) {
	keys := []pivot.Key{
		{Path: "users/*", Database: nil},
		{Path: "orders/*", Database: nil},
		{Path: "products/*", Database: nil},
		{Path: "config", Database: nil},
		{Path: "settings", Database: nil},
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "unknown/path")
	}
}

func BenchmarkFindKeyStorage_TwentyKeys(b *testing.B) {
	keys := make([]pivot.Key, 20)
	for i := range 20 {
		keys[i] = pivot.Key{Path: fmt.Sprintf("path%d/*", i), Database: nil}
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "path15/item")
	}
}

func BenchmarkFindKeyStorage_SingleLevelPath(b *testing.B) {
	keys := []pivot.Key{
		{Path: "data/*", Database: nil},
	}
	b.ResetTimer()
	for b.Loop() {
		pivot.FindKeyStorage(keys, "data/item123")
	}
}

func BenchmarkKey_MatchesIndex(b *testing.B) {
	key := pivot.Key{Path: "users/*"}
	b.ResetTimer()
	for b.Loop() {
		key.MatchesIndex("users/123")
	}
}

func BenchmarkKey_IsGlobPattern_Glob(b *testing.B) {
	key := pivot.Key{Path: "users/*"}
	b.ResetTimer()
	for b.Loop() {
		key.IsGlobPattern()
	}
}

func BenchmarkKey_IsGlobPattern_Exact(b *testing.B) {
	key := pivot.Key{Path: "config"}
	b.ResetTimer()
	for b.Loop() {
		key.IsGlobPattern()
	}
}

func BenchmarkKey_BasePath_Glob(b *testing.B) {
	key := pivot.Key{Path: "users/*"}
	b.ResetTimer()
	for b.Loop() {
		key.BasePath()
	}
}

func BenchmarkKey_BasePath_Exact(b *testing.B) {
	key := pivot.Key{Path: "config"}
	b.ResetTimer()
	for b.Loop() {
		key.BasePath()
	}
}
