package profile

import (
	"testing"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		// Decimal SI
		{"1GB", 1_000_000_000, false},
		{"2GB", 2_000_000_000, false},
		{"512MB", 512_000_000, false},
		{"1MB", 1_000_000, false},
		{"1KB", 1_000, false},
		// Binary IEC
		{"1GiB", 1 << 30, false},
		{"1MiB", 1 << 20, false},
		{"1KiB", 1 << 10, false},
		{"1TiB", 1 << 40, false},
		// Case insensitivity
		{"1gb", 1_000_000_000, false},
		{"512mb", 512_000_000, false},
		{"1gib", 1 << 30, false},
		// Plain bytes
		{"1B", 1, false},
		{"4096", 4096, false},
		// Fractional
		{"1.5GB", 1_500_000_000, false},
		{"0.5GiB", 1 << 29, false},
		// Whitespace
		{"  1GB  ", 1_000_000_000, false},
		// Errors
		{"notanumber GB", 0, true},
		{"", 0, true},
		{"xyz", 0, true},
	}

	for _, tt := range tests {
		got, err := ParseSize(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSize(%q) expected error, got %d", tt.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSize(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseSizeSuffixOrdering(t *testing.T) {
	// Ensure "MB" is not accidentally matched as "B" (longest-suffix-first rule)
	v, err := ParseSize("1MB")
	if err != nil {
		t.Fatal(err)
	}
	if v != 1_000_000 {
		t.Errorf("1MB should be 1_000_000, got %d", v)
	}

	v, err = ParseSize("1MiB")
	if err != nil {
		t.Fatal(err)
	}
	if v != 1<<20 {
		t.Errorf("1MiB should be %d, got %d", 1<<20, v)
	}
}
