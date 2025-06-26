package fileplay_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/yuchanns/fileplay/ffi"
	"github.com/yuchanns/fileplay/opendal"
	"github.com/yuchanns/fileplay/pure"
)

type Size uint64

const (
	_   = iota
	KiB = 1 << (10 * iota)
	MiB
)

func fromKibibytes(kib uint64) Size {
	return Size(kib * KiB)
}

func fromMebibytes(mib uint64) Size {
	return Size(mib * MiB)
}

func (s Size) Bytes() uint64 {
	return uint64(s)
}

func genFixedBytes(size uint) []byte {
	content := make([]byte, size)
	_, _ = rand.Read(content)
	return content
}

// FileCreator defines the interface for creating files
type FileCreator interface {
	Create(path string) (io.ReadWriteCloser, error)
	Open(path string) (io.ReadWriteCloser, error)
}

// OSFileCreator implements FileCreator for os package
type OSFileCreator struct{}

func (c OSFileCreator) Create(path string) (io.ReadWriteCloser, error) {
	return os.Create(path)
}

func (c OSFileCreator) Open(path string) (io.ReadWriteCloser, error) {
	return os.Open(path)
}

// PureCreator implements FileCreator for pure package
type PureCreator struct{}

func (c PureCreator) Create(path string) (io.ReadWriteCloser, error) {
	return pure.Create(path)
}

func (c PureCreator) Open(path string) (io.ReadWriteCloser, error) {
	return pure.Open(path)
}

// FFICreator implements FileCreator for ffi package
type FFICreator struct{}

func (c FFICreator) Create(path string) (io.ReadWriteCloser, error) {
	return ffi.Create(path)
}

func (c FFICreator) Open(path string) (io.ReadWriteCloser, error) {
	return ffi.Open(path)
}

// OpenDALCreator implements FileCreator for OpenDAL
type OpenDALCreator struct{}

func (c OpenDALCreator) Create(path string) (io.ReadWriteCloser, error) {
	return opendal.Create(path)
}

func (c OpenDALCreator) Open(path string) (io.ReadWriteCloser, error) {
	return opendal.Open(path)
}

// runBenchmarkWrite performs generic write benchmark for any FileCreator
func runBenchmarkWrite(b *testing.B, creator FileCreator, size Size) {
	data := genFixedBytes(uint(size.Bytes()))
	path := uuid.NewString()
	b.Cleanup(func() {
		os.Remove(path)
	})

	for b.Loop() {
		file, err := creator.Create(path)
		if err != nil {
			b.Fatalf("Failed to create file: %s", err)
		}

		_, err = file.Write(data)
		if err != nil {
			b.Fatalf("Failed to write: %s", err)
		}

		err = file.Close()
		if err != nil {
			b.Fatalf("Failed to close: %s", err)
		}
	}
}

// runBenchmarkRead performs generic read benchmark for any FileCreator
func runBenchmarkRead(b *testing.B, creator FileCreator, size Size) {
	path := uuid.NewString()
	data := genFixedBytes(uint(size.Bytes()))
	b.Cleanup(func() {
		os.Remove(path)
	})

	// Create test file
	file, err := creator.Create(path)
	if err != nil {
		b.Fatalf("Failed to create file: %s", err)
	}
	_, err = file.Write(data)
	if err != nil {
		b.Fatalf("Failed to write: %s", err)
	}
	err = file.Close()
	if err != nil {
		b.Fatalf("Failed to close: %s", err)
	}

	for b.Loop() {
		file, err := creator.Open(path)
		if err != nil {
			b.Fatalf("Failed to open file: %s", err)
		}

		buffer := make([]byte, size.Bytes())
		_, err = io.ReadFull(file, buffer)
		if err != nil {
			b.Fatalf("Failed to read: %s", err)
		}

		err = file.Close()
		if err != nil {
			b.Fatalf("Failed to close: %s", err)
		}
	}
}

var (
	creators = map[string]FileCreator{
		"opendal": OpenDALCreator{},
		// "pure":    PureCreator{},
		// "ffi":     FFICreator{},
		"os":      OSFileCreator{},
	}

	sizes = map[string]Size{
		"4KiB":   fromKibibytes(4),
		// "256KiB": fromKibibytes(256),
		// "4MiB":   fromMebibytes(4),
		// "16MiB":  fromMebibytes(16),
	}
)

func getSorted() (sizeNames []string, creatorNames []string) {
	for sizeName := range sizes {
		sizeNames = append(sizeNames, sizeName)
	}
	for creatorName := range creators {
		creatorNames = append(creatorNames, creatorName)
	}
	slices.Sort(sizeNames)
	slices.Sort(creatorNames)
	return
}

// BenchmarkFileWrite runs write benchmarks
func BenchmarkFileWrite(b *testing.B) {
	sizeNames, creatorNames := getSorted()
	for sizeName := range sizeNames {
		for creatorName := range creatorNames {
			b.Run(fmt.Sprintf("%s_%s", creatorNames[creatorName], sizeNames[sizeName]), func(b *testing.B) {
				runBenchmarkWrite(b, creators[creatorNames[creatorName]], sizes[sizeNames[sizeName]])
			})
		}
	}
}

// BenchmarkFileRead runs read benchmarks
func BenchmarkFileRead(b *testing.B) {
	sizeNames, creatorNames := getSorted()
	for sizeName := range sizeNames {
		for creatorName := range creatorNames {
			b.Run(fmt.Sprintf("%s_%s", creatorNames[creatorName], sizeNames[sizeName]), func(b *testing.B) {
				runBenchmarkRead(b, creators[creatorNames[creatorName]], sizes[sizeNames[sizeName]])
			})
		}
	}
}
