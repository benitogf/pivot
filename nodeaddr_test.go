package pivot

import "testing"

// TestParseNodeAddr covers the host-field override that lets the leader dial
// nodes by a DNS name (so a name-only TLS leaf validates) while preserving the
// historical ip:port behavior by default.
func TestParseNodeAddr(t *testing.T) {
	cases := []struct {
		name      string
		data      string
		hostField string
		want      string
	}{
		{
			name: "default uses ip lowercase",
			data: `{"ip":"10.0.2.101","port":3099}`,
			want: "10.0.2.101:3099",
		},
		{
			name: "default uses IP uppercase",
			data: `{"IP":"10.0.2.101","Port":3099}`,
			want: "10.0.2.101:3099",
		},
		{
			name: "default ignores host field when not configured",
			data: `{"ip":"10.0.2.101","host":"table-001.internal","port":3099}`,
			want: "10.0.2.101:3099",
		},
		{
			name:      "host field overrides ip when configured and present",
			data:      `{"ip":"10.0.2.101","host":"table-001.internal","port":3099}`,
			hostField: "host",
			want:      "table-001.internal:3099",
		},
		{
			name:      "host field falls back to ip when blank",
			data:      `{"ip":"10.0.2.101","host":"","port":3099}`,
			hostField: "host",
			want:      "10.0.2.101:3099",
		},
		{
			name:      "host field falls back to ip when absent",
			data:      `{"ip":"10.0.2.101","port":3099}`,
			hostField: "host",
			want:      "10.0.2.101:3099",
		},
		{
			name:      "port as quoted string is accepted with host field",
			data:      `{"host":"table-001.internal","port":"3099"}`,
			hostField: "host",
			want:      "table-001.internal:3099",
		},
		{
			name:      "no host and no ip yields empty",
			data:      `{"port":3099}`,
			hostField: "host",
			want:      "",
		},
		{
			name: "missing port yields empty",
			data: `{"ip":"10.0.2.101"}`,
			want: "",
		},
		{
			name: "invalid json yields empty",
			data: `not json`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseNodeAddr([]byte(tc.data), tc.hostField)
			if got != tc.want {
				t.Fatalf("parseNodeAddr(%s, %q) = %q, want %q", tc.data, tc.hostField, got, tc.want)
			}
		})
	}
}
