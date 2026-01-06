package pivot

import (
	"github.com/benitogf/ooo/meta"
)

// GetEntriesNegativeDiff returns entries in objsDst that are not in objsSrc (deletions).
// O(n) using map lookup instead of O(n²) nested loops.
func GetEntriesNegativeDiff(objsDst, objsSrc []meta.Object) []string {
	// Build map of source indices for O(1) lookup
	srcIndices := make(map[string]struct{}, len(objsSrc))
	for _, obj := range objsSrc {
		srcIndices[obj.Index] = struct{}{}
	}

	var result []string
	for _, objDst := range objsDst {
		if _, found := srcIndices[objDst.Index]; !found {
			result = append(result, objDst.Index)
		}
	}
	return result
}

// GetEntriesPositiveDiff returns entries in objsSrc that are new or updated compared to objsDst.
// O(n) using map lookup instead of O(n²) nested loops.
func GetEntriesPositiveDiff(objsDst, objsSrc []meta.Object) []meta.Object {
	// Build map of destination entries for O(1) lookup
	dstEntries := make(map[string]meta.Object, len(objsDst))
	for _, obj := range objsDst {
		dstEntries[obj.Index] = obj
	}

	var result []meta.Object
	for _, objSrc := range objsSrc {
		if objDst, found := dstEntries[objSrc.Index]; !found {
			// New entry - not in destination
			result = append(result, objSrc)
		} else if objSrc.Updated > objDst.Updated {
			// Updated entry - source is newer
			result = append(result, objSrc)
		}
	}
	return result
}
