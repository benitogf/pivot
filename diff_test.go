package pivot_test

import (
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/pivot"
	"github.com/stretchr/testify/require"
)

func TestGetEntriesNegativeDiff_EmptyLists(t *testing.T) {
	result := pivot.GetEntriesNegativeDiff([]meta.Object{}, []meta.Object{})
	require.Len(t, result, 0)
}

func TestGetEntriesNegativeDiff_EmptySource(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
	}
	result := pivot.GetEntriesNegativeDiff(dst, []meta.Object{})
	require.Len(t, result, 2)
	require.Contains(t, result, "a")
	require.Contains(t, result, "b")
}

func TestGetEntriesNegativeDiff_EmptyDestination(t *testing.T) {
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
	}
	result := pivot.GetEntriesNegativeDiff([]meta.Object{}, src)
	require.Len(t, result, 0)
}

func TestGetEntriesNegativeDiff_IdenticalLists(t *testing.T) {
	objs := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
		{Index: "c", Created: 3, Updated: 3},
	}
	result := pivot.GetEntriesNegativeDiff(objs, objs)
	require.Len(t, result, 0)
}

func TestGetEntriesNegativeDiff_AllDeleted(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
		{Index: "c", Created: 3, Updated: 3},
	}
	src := []meta.Object{
		{Index: "x", Created: 10, Updated: 10},
		{Index: "y", Created: 20, Updated: 20},
	}
	result := pivot.GetEntriesNegativeDiff(dst, src)
	require.Len(t, result, 3)
	require.Contains(t, result, "a")
	require.Contains(t, result, "b")
	require.Contains(t, result, "c")
}

func TestGetEntriesNegativeDiff_PartialOverlap(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
		{Index: "c", Created: 3, Updated: 3},
	}
	src := []meta.Object{
		{Index: "b", Created: 2, Updated: 2},
		{Index: "d", Created: 4, Updated: 4},
	}
	result := pivot.GetEntriesNegativeDiff(dst, src)
	require.Len(t, result, 2)
	require.Contains(t, result, "a")
	require.Contains(t, result, "c")
}

func TestGetEntriesPositiveDiff_EmptyLists(t *testing.T) {
	result := pivot.GetEntriesPositiveDiff([]meta.Object{}, []meta.Object{})
	require.Len(t, result, 0)
}

func TestGetEntriesPositiveDiff_EmptySource(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
	}
	result := pivot.GetEntriesPositiveDiff(dst, []meta.Object{})
	require.Len(t, result, 0)
}

func TestGetEntriesPositiveDiff_EmptyDestination(t *testing.T) {
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
	}
	result := pivot.GetEntriesPositiveDiff([]meta.Object{}, src)
	require.Len(t, result, 2)
	require.Equal(t, "a", result[0].Index)
	require.Equal(t, "b", result[1].Index)
}

func TestGetEntriesPositiveDiff_IdenticalLists(t *testing.T) {
	objs := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
		{Index: "c", Created: 3, Updated: 3},
	}
	result := pivot.GetEntriesPositiveDiff(objs, objs)
	require.Len(t, result, 0)
}

func TestGetEntriesPositiveDiff_AllNew(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
	}
	src := []meta.Object{
		{Index: "x", Created: 10, Updated: 10},
		{Index: "y", Created: 20, Updated: 20},
		{Index: "z", Created: 30, Updated: 30},
	}
	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 3)
}

func TestGetEntriesPositiveDiff_UpdatedEntries(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
		{Index: "c", Created: 3, Updated: 3},
	}
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},  // Same - no diff
		{Index: "b", Created: 2, Updated: 10}, // Updated - should be in diff
		{Index: "c", Created: 3, Updated: 3},  // Same - no diff
	}
	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 1)
	require.Equal(t, "b", result[0].Index)
	require.Equal(t, int64(10), result[0].Updated)
}

func TestGetEntriesPositiveDiff_MixedNewAndUpdated(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 1},
		{Index: "b", Created: 2, Updated: 2},
	}
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 5}, // Updated
		{Index: "b", Created: 2, Updated: 2}, // Same
		{Index: "c", Created: 3, Updated: 3}, // New
		{Index: "d", Created: 4, Updated: 4}, // New
	}
	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 3)

	indices := make([]string, len(result))
	for i, obj := range result {
		indices[i] = obj.Index
	}
	require.Contains(t, indices, "a")
	require.Contains(t, indices, "c")
	require.Contains(t, indices, "d")
}

func TestGetEntriesPositiveDiff_OlderSourceIgnored(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 10},
		{Index: "b", Created: 2, Updated: 20},
	}
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 5},  // Older - should be ignored
		{Index: "b", Created: 2, Updated: 15}, // Older - should be ignored
	}
	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 0)
}

func TestGetEntriesPositiveDiff_SameTimestampIgnored(t *testing.T) {
	dst := []meta.Object{
		{Index: "a", Created: 1, Updated: 10},
	}
	src := []meta.Object{
		{Index: "a", Created: 1, Updated: 10}, // Same timestamp - should be ignored
	}
	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 0)
}

func TestGetEntriesNegativeDiff_LargeList(t *testing.T) {
	// Test with larger lists to ensure O(n) performance
	dst := make([]meta.Object, 1000)
	src := make([]meta.Object, 500)

	for i := 0; i < 1000; i++ {
		dst[i] = meta.Object{Index: string(rune('a' + i)), Created: int64(i), Updated: int64(i)}
	}
	for i := 0; i < 500; i++ {
		src[i] = meta.Object{Index: string(rune('a' + i*2)), Created: int64(i * 2), Updated: int64(i * 2)}
	}

	result := pivot.GetEntriesNegativeDiff(dst, src)
	require.NotNil(t, result)
}

func TestGetEntriesPositiveDiff_LargeList(t *testing.T) {
	// Test with larger lists to ensure O(n) performance
	dst := make([]meta.Object, 500)
	src := make([]meta.Object, 1000)

	for i := 0; i < 500; i++ {
		dst[i] = meta.Object{Index: string(rune('a' + i)), Created: int64(i), Updated: int64(i)}
	}
	for i := 0; i < 1000; i++ {
		src[i] = meta.Object{Index: string(rune('a' + i)), Created: int64(i), Updated: int64(i + 1)}
	}

	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.NotNil(t, result)
}

func TestGetEntriesNegativeDiff_PreservesOrder(t *testing.T) {
	dst := []meta.Object{
		{Index: "z", Created: 1, Updated: 1},
		{Index: "y", Created: 2, Updated: 2},
		{Index: "x", Created: 3, Updated: 3},
	}
	src := []meta.Object{}

	result := pivot.GetEntriesNegativeDiff(dst, src)
	require.Len(t, result, 3)
	// Order should be preserved from dst iteration
	require.Equal(t, "z", result[0])
	require.Equal(t, "y", result[1])
	require.Equal(t, "x", result[2])
}

func TestGetEntriesPositiveDiff_PreservesOrder(t *testing.T) {
	dst := []meta.Object{}
	src := []meta.Object{
		{Index: "z", Created: 1, Updated: 1},
		{Index: "y", Created: 2, Updated: 2},
		{Index: "x", Created: 3, Updated: 3},
	}

	result := pivot.GetEntriesPositiveDiff(dst, src)
	require.Len(t, result, 3)
	// Order should be preserved from src iteration
	require.Equal(t, "z", result[0].Index)
	require.Equal(t, "y", result[1].Index)
	require.Equal(t, "x", result[2].Index)
}
