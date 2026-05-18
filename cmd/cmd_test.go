package cmd

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/minio/pulsar/profile"
)

// ---------------------------------------------------------------------------
// version
// ---------------------------------------------------------------------------

func TestSetVersion(t *testing.T) {
	SetVersion("v1.2.3")
	if buildVersion != "v1.2.3" {
		t.Errorf("buildVersion = %q, want %q", buildVersion, "v1.2.3")
	}
	SetVersion("dev") // restore
}

// ---------------------------------------------------------------------------
// profile loading — exercises the same code path the CLI uses
// ---------------------------------------------------------------------------

func TestLoadBuiltin_AllProfiles(t *testing.T) {
	for _, p := range profile.Builtin() {
		loaded, err := profile.LoadBuiltin(p.Name)
		if err != nil {
			t.Errorf("LoadBuiltin(%q): %v", p.Name, err)
			continue
		}
		if loaded.Workers <= 0 {
			t.Errorf("profile %q: Workers = %d, must be > 0", p.Name, loaded.Workers)
		}
		if loaded.Duration <= 0 {
			t.Errorf("profile %q: Duration = %v, must be > 0", p.Name, loaded.Duration)
		}
		if loaded.Files.Count <= 0 {
			t.Errorf("profile %q: Files.Count = %d, must be > 0", p.Name, loaded.Files.Count)
		}
		if loaded.Files.SizeBytes <= 0 {
			t.Errorf("profile %q: Files.SizeBytes = %d, must be > 0", p.Name, loaded.Files.SizeBytes)
		}
		if loaded.BlockSize <= 0 {
			t.Errorf("profile %q: BlockSize = %d, must be > 0", p.Name, loaded.BlockSize)
		}
	}
}

func TestLoadBuiltin_Unknown(t *testing.T) {
	_, err := profile.LoadBuiltin("does-not-exist")
	if err == nil {
		t.Error("expected error for unknown profile name, got nil")
	}
}

func TestLoadFile_ValidYAML(t *testing.T) {
	yaml := `name: test-profile
workload: sequential-read
workers: 4
duration: 10s
files:
  count: 2
  size: 100MB
block_size: 1MB
cleanup: true
`
	tmp := t.TempDir() + "/profile.yaml"
	if err := os.WriteFile(tmp, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	p, err := profile.LoadFile(tmp)
	if err != nil {
		t.Fatalf("LoadFile error: %v", err)
	}
	if p.Name != "test-profile" {
		t.Errorf("Name = %q, want test-profile", p.Name)
	}
	if p.Workers != 4 {
		t.Errorf("Workers = %d, want 4", p.Workers)
	}
	if p.BlockSize != 1_000_000 {
		t.Errorf("BlockSize = %d, want 1_000_000 (1MB)", p.BlockSize)
	}
	if p.Files.SizeBytes != 100_000_000 {
		t.Errorf("Files.SizeBytes = %d, want 100_000_000 (100MB)", p.Files.SizeBytes)
	}
}

func TestLoadFile_UnknownWorkload(t *testing.T) {
	yaml := `name: bad
workload: sequential-reads
workers: 4
duration: 10s
files:
  count: 2
  size: 100MB
`
	tmp := t.TempDir() + "/bad.yaml"
	if err := os.WriteFile(tmp, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := profile.LoadFile(tmp)
	if err == nil {
		t.Error("expected error for unknown workload type, got nil")
	}
	if !strings.Contains(err.Error(), "sequential-reads") {
		t.Errorf("error should mention bad workload name, got: %v", err)
	}
}

func TestLoadFile_BlockSizeStringFormats(t *testing.T) {
	tests := []struct {
		input     string
		wantBytes int64
	}{
		{"256KB", 256_000},
		{"4MB", 4_000_000},
		{"1MiB", 1 << 20},
		{"512KiB", 512 << 10},
		{"262144", 262144}, // plain integer bytes
	}
	for _, tt := range tests {
		yaml := "name: bs-test\nworkload: sequential-read\nworkers: 1\nduration: 1s\nfiles:\n  count: 1\n  size: 100MB\nblock_size: " + tt.input + "\n"
		tmp := t.TempDir() + "/bs.yaml"
		if err := os.WriteFile(tmp, []byte(yaml), 0644); err != nil {
			t.Fatal(err)
		}
		p, err := profile.LoadFile(tmp)
		if err != nil {
			t.Errorf("block_size %q: unexpected error: %v", tt.input, err)
			continue
		}
		if p.BlockSize != tt.wantBytes {
			t.Errorf("block_size %q: BlockSize = %d, want %d", tt.input, p.BlockSize, tt.wantBytes)
		}
	}
}

func TestLoadFile_MissingWorkload(t *testing.T) {
	yaml := `name: noworkload
workers: 4
duration: 10s
files:
  count: 2
  size: 100MB
`
	tmp := t.TempDir() + "/nw.yaml"
	if err := os.WriteFile(tmp, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := profile.LoadFile(tmp)
	if err == nil {
		t.Error("expected error for missing workload field, got nil")
	}
}

// ---------------------------------------------------------------------------
// list command — verify all profiles appear in output
// ---------------------------------------------------------------------------

func TestListCmd_AllProfilesPresent(t *testing.T) {
	buf := &bytes.Buffer{}
	listCmd.SetOut(buf)
	listCmd.SetErr(buf)
	listCmd.Run(listCmd, nil)

	out := buf.String()
	for _, p := range profile.Builtin() {
		if !strings.Contains(out, p.Name) {
			t.Errorf("list output missing profile %q", p.Name)
		}
		if !strings.Contains(out, p.Focus) {
			t.Errorf("list output missing focus %q for profile %q", p.Focus, p.Name)
		}
	}
}

func TestListCmd_NoMlperfNames(t *testing.T) {
	buf := &bytes.Buffer{}
	listCmd.SetOut(buf)
	listCmd.SetErr(buf)
	listCmd.Run(listCmd, nil)

	if strings.Contains(strings.ToLower(buf.String()), "mlperf") {
		t.Error("list output must not contain 'mlperf'")
	}
}
