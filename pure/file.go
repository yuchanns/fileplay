package pure

import (
	"fmt"
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
	libcFseek  func(stream uintptr, offset int, whence int) int
	libcFtell  func(stream uintptr) int
	libcRewind func(stream uintptr)

	// File status functions
	libcAccess func(pathname *byte, mode int) int
	libcRemove func(filename *byte) int
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
	purego.RegisterLibFunc(&libcFseek, libc, "fseek")
	purego.RegisterLibFunc(&libcFtell, libc, "ftell")
	purego.RegisterLibFunc(&libcRewind, libc, "rewind")
	purego.RegisterLibFunc(&libcAccess, libc, "access")
	purego.RegisterLibFunc(&libcRemove, libc, "remove")
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
		return nil, fmt.Errorf("invalid filename: %v", err)
	}

	modePtr, err := unix.BytePtrFromString(mode)
	if err != nil {
		return nil, fmt.Errorf("invalid mode: %v", err)
	}

	stream := libcFopen(namePtr, modePtr)
	if stream == 0 {
		return nil, fmt.Errorf("failed to open file %s", name)
	}

	return &File{
		stream: stream,
		name:   name,
	}, nil
}

// Close closes the file
func (f *File) Close() error {
	if f.stream == 0 {
		return fmt.Errorf("file already closed")
	}

	ret := libcFclose(f.stream)
	if ret != 0 {
		return fmt.Errorf("failed to close file")
	}

	f.stream = 0
	return nil
}

// Read reads data into buffer
func (f *File) Read(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, fmt.Errorf("file is closed")
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := libcFread(unsafe.Pointer(&p[0]), 1, uintptr(len(p)), f.stream)
	return int(count), nil
}

// Write writes data from buffer to file
func (f *File) Write(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, fmt.Errorf("file is closed")
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
