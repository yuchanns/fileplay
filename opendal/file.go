package opendal

import (
	"io"
	"log"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

// Define opendal function signatures
var (
	opendalWriter      func(path *byte) uintptr
	opendalReader      func(path *byte) uintptr
	opendalWriterFree  func(writer uintptr)
	opendalReaderFree  func(reader uintptr)
	opendalWriterWrite func(writer uintptr, data *uint8, len uintptr) int32
	opendalReaderRead  func(reader uintptr, data *uint8, len uintptr) int32
)

func init() {
	// Load opendal library
	var err error
	var lib uintptr
	switch runtime.GOOS {
	case "linux":
		lib, err = purego.Dlopen("opendal/target/debug/libopendal_c.so", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	case "darwin":
		lib, err = purego.Dlopen("opendal/target/debug/libopendal_c.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	}
	if err != nil {
		log.Fatal("Failed to load opendal library:", err)
	}

	// Get function addresses and register them
	purego.RegisterLibFunc(&opendalWriter, lib, "opendal_writer")
	purego.RegisterLibFunc(&opendalReader, lib, "opendal_reader")
	purego.RegisterLibFunc(&opendalWriterFree, lib, "opendal_writer_free")
	purego.RegisterLibFunc(&opendalReaderFree, lib, "opendal_reader_free")
	purego.RegisterLibFunc(&opendalWriterWrite, lib, "opendal_writer_write")
	purego.RegisterLibFunc(&opendalReaderRead, lib, "opendal_reader_read")
}

// File structure similar to os.File
type File struct {
	reader uintptr // opendal_reader pointer
	writer uintptr // opendal_writer pointer
	name   string  // filename
}

var _ io.ReadWriteCloser = (*File)(nil)

// Open opens a file for reading
func Open(name string) (*File, error) {
	return OpenFile(name, "r")
}

// Create creates a file for writing
func Create(name string) (*File, error) {
	// For opendal, we use the same open function
	// The underlying implementation should handle creation
	return OpenFile(name, "w")
}

// OpenFile opens a file with the specified mode (for compatibility)
func OpenFile(name, mode string) (*File, error) {
	namePtr, err := unix.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}

	file := &File{
		name: name,
	}

	// Create reader and/or writer based on mode
	switch mode {
	case "r":
		// Read-only mode
		file.reader = opendalReader(namePtr)
		if file.reader == 0 {
			return nil, unix.EINVAL
		}
	case "w":
		// Write-only mode
		file.writer = opendalWriter(namePtr)
		if file.writer == 0 {
			return nil, unix.EINVAL
		}
	default:
		return nil, unix.EINVAL
	}

	return file, nil
}

// Close closes the file
func (f *File) Close() error {
	// Free reader if it exists
	if f.reader != 0 {
		opendalReaderFree(f.reader)
		f.reader = 0
	}

	// Free writer if it exists
	if f.writer != 0 {
		opendalWriterFree(f.writer)
		f.writer = 0
	}

	return nil
}

// Read reads data into buffer
func (f *File) Read(p []byte) (n int, err error) {
	if f.reader == 0 {
		return 0, unix.EBADF // file is closed or not opened for reading
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := opendalReaderRead(f.reader, (*uint8)(unsafe.Pointer(&p[0])), uintptr(len(p)))
	if count < 0 {
		return 0, unix.EINVAL // read error
	}
	if int(count) < len(p) {
		return int(count), io.EOF // no more data to read
	}
	return int(count), nil
}

// Write writes data from buffer to file
func (f *File) Write(p []byte) (n int, err error) {
	if f.writer == 0 {
		return 0, unix.EBADF // file is closed or not opened for writing
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := opendalWriterWrite(f.writer, (*uint8)(unsafe.Pointer(&p[0])), uintptr(len(p)))
	return int(count), nil
}

// Name returns the name of the file
func (f *File) Name() string {
	return f.name
}
