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
	// Build map of destination updated timestamps for O(1) lookup.
	// We only need Updated for the comparison, so avoid copying full
	// meta.Object values (which include RawMessage payloads).
	dstUpdated := make(map[string]int64, len(objsDst))
	for _, obj := range objsDst {
		dstUpdated[obj.Index] = obj.Updated
	}

	var result []meta.Object
	for _, objSrc := range objsSrc {
		if updatedDst, found := dstUpdated[objSrc.Index]; !found {
			// New entry - not in destination
			result = append(result, objSrc)
		} else if objSrc.Updated > updatedDst {
			// Updated entry - source is newer
			result = append(result, objSrc)
		}
	}
	return result
}
