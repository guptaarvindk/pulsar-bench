package profile

import (
	"math/rand"
	"testing"
)

func TestGenerateFileSizes_Uniform(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sizes := GenerateFileSizes("", 10, 1<<30, rng) // 1 GiB
	if len(sizes) != 10 {
		t.Fatalf("expected 10 sizes, got %d", len(sizes))
	}
	want := align4k(1 << 30)
	for i, s := range sizes {
		if s != want {
			t.Errorf("sizes[%d] = %d, want %d", i, s, want)
		}
		if s%4096 != 0 {
			t.Errorf("sizes[%d] = %d is not 4K-aligned", i, s)
		}
	}
}

func TestGenerateFileSizes_Imagenet(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sizes := GenerateFileSizes("imagenet", 1000, 120*1024, rng)
	if len(sizes) != 1000 {
		t.Fatalf("expected 1000 sizes, got %d", len(sizes))
	}
	var sum int64
	for i, s := range sizes {
		if s < 4096 {
			t.Errorf("sizes[%d] = %d is below 4096 minimum", i, s)
		}
		if s > 50*1024*1024 {
			t.Errorf("sizes[%d] = %d exceeds 50MB cap", i, s)
		}
		if s%4096 != 0 {
			t.Errorf("sizes[%d] = %d is not 4K-aligned", i, s)
		}
		sum += s
	}
	// Mean should be roughly near 120KB (log-normal, so expect wide variance)
	meanBytes := sum / int64(len(sizes))
	if meanBytes < 50*1024 || meanBytes > 500*1024 {
		t.Errorf("imagenet mean = %d bytes, expected roughly 120KB range [50KB, 500KB]", meanBytes)
	}
}

func TestGenerateFileSizes_Bert(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	sizes := GenerateFileSizes("bert", 5, 0, rng)
	if len(sizes) != 5 {
		t.Fatalf("expected 5 sizes, got %d", len(sizes))
	}
	want := align4k(4_300_000_000)
	for i, s := range sizes {
		if s != want {
			t.Errorf("bert sizes[%d] = %d, want %d", i, s, want)
		}
		if s%4096 != 0 {
			t.Errorf("bert sizes[%d] is not 4K-aligned", i)
		}
	}
}

func TestGenerateFileSizes_Unet(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	sizes := GenerateFileSizes("unet", 8, 0, rng)
	if len(sizes) != 8 {
		t.Fatalf("expected 8 sizes, got %d", len(sizes))
	}
	want := align4k(150_000_000)
	for i, s := range sizes {
		if s != want {
			t.Errorf("unet sizes[%d] = %d, want %d", i, s, want)
		}
		if s%4096 != 0 {
			t.Errorf("unet sizes[%d] is not 4K-aligned", i)
		}
	}
}

func TestGenerateFileSizes_Deterministic(t *testing.T) {
	// Same seed must produce same sizes
	sizes1 := GenerateFileSizes("imagenet", 100, 120*1024, rand.New(rand.NewSource(99)))
	sizes2 := GenerateFileSizes("imagenet", 100, 120*1024, rand.New(rand.NewSource(99)))
	for i := range sizes1 {
		if sizes1[i] != sizes2[i] {
			t.Errorf("sizes differ at index %d: %d vs %d", i, sizes1[i], sizes2[i])
		}
	}
}

func TestAlign4k(t *testing.T) {
	tests := []struct{ in, want int64 }{
		{0, 0},
		{1, 4096},
		{4095, 4096},
		{4096, 4096},
		{4097, 8192},
		{1 << 20, 1 << 20},
	}
	for _, tt := range tests {
		if got := align4k(tt.in); got != tt.want {
			t.Errorf("align4k(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
