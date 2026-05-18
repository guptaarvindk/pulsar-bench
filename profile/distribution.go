package profile

import (
	"math"
	"math/rand"
)

// GenerateFileSizes returns a slice of file sizes for the given distribution and count.
// All sizes are rounded to 4096-byte boundaries for O_DIRECT compatibility.
func GenerateFileSizes(dist string, count int, baseSizeBytes int64, rng *rand.Rand) []int64 {
	switch dist {
	case "imagenet":
		return logNormalSizes(count, 120*1024, 0.7, rng) // mean=120KB, σ=0.7 in log-space
	case "bert":
		// BERT-pattern: large HDF5-equivalent sequential files.
		// Uses baseSizeBytes from the profile when set (non-zero),
		// otherwise falls back to the canonical 4.3 GB per file.
		bertSize := int64(4_300_000_000)
		if baseSizeBytes > 0 {
			bertSize = baseSizeBytes
		}
		sizes := make([]int64, count)
		for i := range sizes {
			sizes[i] = align4k(bertSize)
		}
		return sizes
	case "unet":
		// 3D-UNet pattern: volumetric medical imaging files.
		// Uses baseSizeBytes from the profile when set (non-zero),
		// otherwise falls back to the canonical 150 MB per file.
		unetSize := int64(150_000_000)
		if baseSizeBytes > 0 {
			unetSize = baseSizeBytes
		}
		sizes := make([]int64, count)
		for i := range sizes {
			sizes[i] = align4k(unetSize)
		}
		return sizes
	default: // "uniform"
		sizes := make([]int64, count)
		for i := range sizes {
			sizes[i] = align4k(baseSizeBytes)
		}
		return sizes
	}
}

func logNormalSizes(count int, meanBytes float64, sigma float64, rng *rand.Rand) []int64 {
	mu := math.Log(meanBytes) - sigma*sigma/2
	sizes := make([]int64, count)
	for i := range sizes {
		v := math.Exp(mu + sigma*rng.NormFloat64())
		if v < 4096 {
			v = 4096
		}
		if v > 50*1024*1024 {
			v = 50 * 1024 * 1024 // cap at 50MB (ImageNet max)
		}
		sizes[i] = align4k(int64(v))
	}
	return sizes
}

func align4k(n int64) int64 {
	return (n + 4095) &^ 4095
}
