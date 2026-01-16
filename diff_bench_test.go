package pivot_test

import (
	"fmt"
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/pivot"
)

func generateObjects(n int) []meta.Object {
	objs := make([]meta.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = meta.Object{
			Index:   fmt.Sprintf("item_%d", i),
			Created: int64(i),
			Updated: int64(i),
			Data:    []byte(fmt.Sprintf(`{"id":%d}`, i)),
		}
	}
	return objs
}

func generateObjectsWithOffset(n, offset int) []meta.Object {
	objs := make([]meta.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = meta.Object{
			Index:   fmt.Sprintf("item_%d", i+offset),
			Created: int64(i + offset),
			Updated: int64(i + offset),
			Data:    []byte(fmt.Sprintf(`{"id":%d}`, i+offset)),
		}
	}
	return objs
}

func BenchmarkGetEntriesNegativeDiff_Small(b *testing.B) {
	dst := generateObjects(10)
	src := generateObjects(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(dst, src)
	}
}

func BenchmarkGetEntriesNegativeDiff_Medium(b *testing.B) {
	dst := generateObjects(100)
	src := generateObjects(50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(dst, src)
	}
}

func BenchmarkGetEntriesNegativeDiff_Large(b *testing.B) {
	dst := generateObjects(1000)
	src := generateObjects(500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(dst, src)
	}
}

func BenchmarkGetEntriesNegativeDiff_VeryLarge(b *testing.B) {
	dst := generateObjects(10000)
	src := generateObjects(5000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(dst, src)
	}
}

func BenchmarkGetEntriesNegativeDiff_NoOverlap(b *testing.B) {
	dst := generateObjects(1000)
	src := generateObjectsWithOffset(1000, 1000) // No overlap
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(dst, src)
	}
}

func BenchmarkGetEntriesNegativeDiff_FullOverlap(b *testing.B) {
	objs := generateObjects(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesNegativeDiff(objs, objs)
	}
}

func BenchmarkGetEntriesPositiveDiff_Small(b *testing.B) {
	dst := generateObjects(10)
	src := generateObjects(15)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_Medium(b *testing.B) {
	dst := generateObjects(100)
	src := generateObjects(150)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_Large(b *testing.B) {
	dst := generateObjects(1000)
	src := generateObjects(1500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_VeryLarge(b *testing.B) {
	dst := generateObjects(10000)
	src := generateObjects(15000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_AllNew(b *testing.B) {
	dst := generateObjects(1000)
	src := generateObjectsWithOffset(1000, 1000) // All new entries
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_AllUpdated(b *testing.B) {
	dst := generateObjects(1000)
	// Same indices but updated timestamps
	src := make([]meta.Object, 1000)
	for i := 0; i < 1000; i++ {
		src[i] = meta.Object{
			Index:   fmt.Sprintf("item_%d", i),
			Created: int64(i),
			Updated: int64(i + 1000), // Newer timestamp
			Data:    []byte(fmt.Sprintf(`{"id":%d}`, i)),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(dst, src)
	}
}

func BenchmarkGetEntriesPositiveDiff_NoChanges(b *testing.B) {
	objs := generateObjects(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pivot.GetEntriesPositiveDiff(objs, objs)
	}
}
