package evm

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/erpc/erpc/util"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func init() { util.ConfigureTestLogger() }

func compressionTestCache(t testing.TB) *EvmJsonRpcCache {
	t.Helper()
	return &EvmJsonRpcCache{
		logger: &log.Logger, compressionEnabled: true, compressionThreshold: 512,
		encoderPool: &sync.Pool{New: func() interface{} {
			e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
			require.NoError(t, err)
			return e
		}},
		decoderPool: &sync.Pool{New: func() interface{} {
			d, err := zstd.NewReader(nil)
			require.NoError(t, err)
			return d
		}},
	}
}

func TestCacheCompressionCompatibility(t *testing.T) {
	c := compressionTestCache(t)
	random := make([]byte, 1<<20)
	_, err := rand.New(rand.NewSource(1)).Read(random)
	require.NoError(t, err)
	for name, payload := range map[string][]byte{
		"empty":          nil,
		"small":          []byte(`{"unknown":"method"}`),
		"incompressible": random,
		"large":          bytes.Repeat([]byte(`{"address":"0x1234","data":"0xabcdef","topics":[]},`), 400000),
	} {
		t.Run(name, func(t *testing.T) {
			encoded, compressed := c.compressValueBytes(payload)
			if name == "large" {
				require.True(t, compressed)
			}
			if name != "large" {
				require.False(t, compressed)
			}
			decoded, err := c.decompressValueBytes(encoded)
			require.NoError(t, err)
			require.Equal(t, payload, decoded)
			// Reusing the encoder must not mutate an earlier cached value.
			c.compressValueBytes(bytes.Repeat([]byte("other"), 10000))
			decoded, err = c.decompressValueBytes(encoded)
			require.NoError(t, err)
			require.Equal(t, payload, decoded)
		})
	}
	// Existing streaming cache entries remain readable.
	var legacy bytes.Buffer
	e, err := zstd.NewWriter(&legacy)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("legacy response"), 10000)
	_, err = e.Write(payload)
	require.NoError(t, err)
	require.NoError(t, e.Close())
	decoded, err := c.decompressValueBytes(legacy.Bytes())
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	joined := append(bytes.Clone(legacy.Bytes()), legacy.Bytes()...)
	decoded, err = c.decompressValueBytes(joined)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(payload, 2), decoded)
	_, err = c.decompressValueBytes(legacy.Bytes()[:legacy.Len()-1])
	require.Error(t, err)
	// A failed decode must not poison the pooled decoder.
	decoded, err = c.decompressValueBytes(legacy.Bytes())
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
}

func BenchmarkCacheDecompression(b *testing.B) {
	for _, size := range []int{64 << 10, 16 << 20} {
		rng := rand.New(rand.NewSource(1))
		var input bytes.Buffer
		for input.Len() < size {
			var raw [32]byte
			_, err := rng.Read(raw[:])
			require.NoError(b, err)
			fmt.Fprintf(&input, `{"address":"0x0123456789abcdef0123456789abcdef01234567","blockNumber":"0x123456","data":"0x%s","topics":[]},`, hex.EncodeToString(raw[:]))
		}
		payload := input.Bytes()[:size]
		c := compressionTestCache(b)
		encoded, compressed := c.compressValueBytes(payload)
		require.True(b, compressed)
		for _, direct := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/direct=%t", size, direct), func(b *testing.B) {
				d, err := zstd.NewReader(nil)
				require.NoError(b, err)
				defer d.Close()
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					require.NoError(b, d.Reset(bytes.NewReader(encoded)))
					if direct {
						var buf bytes.Buffer
						_, err := d.WriteTo(&buf)
						require.NoError(b, err)
						if buf.Len() != size {
							b.Fatal("incorrect decoded size")
						}
					} else {
						decoded, err := io.ReadAll(d)
						require.NoError(b, err)
						if len(decoded) != size {
							b.Fatal("incorrect decoded size")
						}
					}
				}
			})
		}
	}
}
