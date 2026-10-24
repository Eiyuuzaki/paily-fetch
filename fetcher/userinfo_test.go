package fetcher

import "testing"

func TestParseUserinfo(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   *UsageInfo
	}{
		{
			name:   "empty header",
			header: "",
			want:   nil,
		},
		{
			name:   "no recognisable keys",
			header: "foo=1; bar=2",
			want:   nil,
		},
		{
			name:   "standard airport header",
			header: "upload=408539; download=555091; total=58339622912; expire=2108019779",
			want:   &UsageInfo{Upload: 408539, Download: 555091, Total: 58339622912, Expire: 2108019779},
		},
		{
			name:   "usage-only header (free plan keeps counters at zero)",
			header: "upload=0; download=0; total=85899345920; expire=2108203925",
			want:   &UsageInfo{Upload: 0, Download: 0, Total: 85899345920, Expire: 2108203925},
		},
		{
			name:   "missing expire",
			header: "upload=1; download=2; total=3",
			want:   &UsageInfo{Upload: 1, Download: 2, Total: 3},
		},
		{
			name:   "no spaces and upper-case keys",
			header: "UPLOAD=10,Download=20,TOTAL=30,EXPIRE=40",
			want:   &UsageInfo{Upload: 10, Download: 20, Total: 30, Expire: 40},
		},
		{
			name:   "non-numeric values ignored",
			header: "upload=abc; total=456",
			want:   &UsageInfo{Total: 456},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseUserinfo(tt.header)
			switch {
			case got == nil && tt.want == nil:
				// ok
			case got == nil || tt.want == nil:
				t.Fatalf("parseUserinfo(%q) = %v, want %v", tt.header, got, tt.want)
			case *got != *tt.want:
				t.Fatalf("parseUserinfo(%q) = %+v, want %+v", tt.header, *got, *tt.want)
			}
		})
	}
}
