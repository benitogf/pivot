package pivot

import (
	"encoding/json"
	"testing"
)

// TestEncodeVVMatchesJSONMarshal pins the invariant that the on-disk byte
// representation produced by encodeVV is identical to what encoding/json
// produced before. This guards against accidental format drift that would
// silently change persisted bytes (and make new writes diverge from old
// writes for the same logical value).
func TestEncodeVVMatchesJSONMarshal(t *testing.T) {
	cases := []struct {
		name string
		vv   VersionVector
	}{
		{"nil", nil},
		{"empty", VersionVector{}},
		{"single", VersionVector{"leader": 1}},
		{"multi", VersionVector{"leader": 5, "nodeA": 2, "nodeB": 9}},
		{"unsorted_input", VersionVector{"zzz": 1, "aaa": 2, "mmm": 3}},
		{"large_values", VersionVector{"leader": 1 << 60, "nodeA": -1}},
		{"address_keys", VersionVector{"10.0.0.1:8080": 4, "10.0.0.2:8080": 7}},
		{"single_zero", VersionVector{"leader": 0}},
		// Cases below pin the escape coverage. The IDs we use as VV keys never
		// contain these characters, but the encoder claims byte-equivalence
		// with json.Marshal (HTMLEscape default) and these pin that contract.
		{"html_unsafe_chars", VersionVector{"<html>": 1, "a&b": 2, ">": 3}},
		{"control_chars", VersionVector{"\x00ctrl": 1, "tab\there": 2, "line\nfeed": 3, "\b\f\r": 4}},
		{"backslash_quote", VersionVector{`back\slash`: 1, `quote"in`: 2}},
		{"high_bit_passthrough", VersionVector{"café": 1, "日本語": 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := json.Marshal(tc.vv)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			got := encodeVV(tc.vv)
			if string(got) != string(want) {
				t.Fatalf("encodeVV mismatch:\n got  = %s\n want = %s", got, want)
			}

			// Round-trip: decoding the encoded bytes must yield an equivalent VV.
			var decoded VersionVector
			if len(tc.vv) > 0 {
				if err := json.Unmarshal(got, &decoded); err != nil {
					t.Fatalf("json.Unmarshal of encodeVV output: %v", err)
				}
				for k, v := range tc.vv {
					if decoded[k] != v {
						t.Fatalf("round-trip drift: key=%q got=%d want=%d", k, decoded[k], v)
					}
				}
				for k := range decoded {
					if _, ok := tc.vv[k]; !ok {
						t.Fatalf("round-trip introduced extra key: %q", k)
					}
				}
			}
		})
	}
}
