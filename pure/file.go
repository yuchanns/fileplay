package pure

import (
	"io"
	"log"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

// Define libc function signatures
var (
	// File operation functions (fopen family)
	libcFopen  func(filename *byte, mode *byte) uintptr // Returns FILE* pointer
	libcFclose func(stream uintptr) int
	libcFread  func(ptr unsafe.Pointer, size, nmemb uintptr, stream uintptr) uintptr
	libcFwrite func(ptr unsafe.Pointer, size, nmemb uintptr, stream uintptr) uintptr
)

// Constants definition (macOS/Linux compatible)
const (
	F_OK = 0 // File exists
	R_OK = 4 // Read permission
	W_OK = 2 // Write permission
	X_OK = 1 // Execute permission

	SEEK_SET = 0
	SEEK_CUR = 1
	SEEK_END = 2
)

func init() {
	// Load libc library
	var err error
	var libc uintptr
	switch runtime.GOOS {
	case "linux":
		libc, err = purego.Dlopen("libc.so.6", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	case "darwin":
		libc, err = purego.Dlopen("libc.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	}
	if err != nil {
		log.Fatal("Failed to load libc:", err)
	}

	// Get function addresses and register them
	purego.RegisterLibFunc(&libcFopen, libc, "fopen")
	purego.RegisterLibFunc(&libcFclose, libc, "fclose")
	purego.RegisterLibFunc(&libcFread, libc, "fread")
	purego.RegisterLibFunc(&libcFwrite, libc, "fwrite")
}

// File structure similar to os.File
type File struct {
	stream uintptr // FILE* pointer
	name   string  // filename
}

var _ io.ReadWriteCloser = (*File)(nil)

func Open(name string) (*File, error) {
	return OpenFile(name, "r")
}

// Create creates a file, similar to os.Create
func Create(name string) (*File, error) {
	return OpenFile(name, "w")
}

// OpenFile opens a file with the specified mode
func OpenFile(name, mode string) (*File, error) {
	namePtr, err := unix.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}

	modePtr, err := unix.BytePtrFromString(mode)
	if err != nil {
		return nil, err
	}

	stream := libcFopen(namePtr, modePtr)
	if stream == 0 {
		return nil, unix.EINVAL // or some other error
	}

	return &File{
		stream: stream,
		name:   name,
	}, nil
}

// Close closes the file
func (f *File) Close() error {
	if f.stream == 0 {
		return nil // already closed
	}

	ret := libcFclose(f.stream)
	if ret != 0 {
		return unix.EINVAL // failed to close
	}

	f.stream = 0
	return nil
}

// Read reads data into buffer
func (f *File) Read(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, unix.EBADF // file is closed
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := libcFread(unsafe.Pointer(&p[0]), 1, uintptr(len(p)), f.stream)
	if int(count) < len(p) {
		return int(count), io.EOF // end of file reached
	}
	return int(count), nil
}

// Write writes data from buffer to file
func (f *File) Write(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, unix.EBADF // file is closed
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := libcFwrite(unsafe.Pointer(&p[0]), 1, uintptr(len(p)), f.stream)
	return int(count), nil
}

// Name returns the name of the file
func (f *File) Name() string {
	return f.name
}
