package opendal

import (
	"context"
	"errors"
	"io"
	"log"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

type ffiOpts struct {
	sym    contextKey
	rType  *ffi.Type
	aTypes []*ffi.Type
}

type ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

type withFFI func(lib uintptr) error

type FFI[T any] struct {
	opts     ffiOpts
	withFunc func(ffiCall ffiCall) T
	sym      T
}

func newFFI[T any](opts ffiOpts, withFunc func(ffiCall ffiCall) T) *FFI[T] {
	ffi := &FFI[T]{
		opts:     opts,
		withFunc: withFunc,
	}
	withFFIs = append(withFFIs, ffi.withFFI)
	return ffi
}

func (f *FFI[T]) symbol() T {
	return f.sym
}

func (f *FFI[T]) withFFI(lib uintptr) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif,
		ffi.DefaultAbi,
		uint32(len(f.opts.aTypes)),
		f.opts.rType,
		f.opts.aTypes...,
	); status != ffi.OK {
		return errors.New(status.String())
	}
	fn, err := GetProcAddress(lib, f.opts.sym.String())
	if err != nil {
		return err
	}
	f.sym = f.withFunc(func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		ffi.Call(&cif, fn, rValue, aValues...)
	})
	return nil
}

var withFFIs []withFFI

func initFFI(path string) (cancel context.CancelFunc, err error) {
	lib, err := LoadLibrary(path)
	if err != nil {
		return
	}
	for _, withFFI := range withFFIs {
		err = withFFI(lib)
		if err != nil {
			return
		}
	}
	cancel = func() {
		_ = FreeLibrary(lib)
	}
	return
}

func LoadLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
}

func FreeLibrary(handle uintptr) error {
	if handle == 0 {
		return nil
	}
	err := purego.Dlclose(handle)
	if err != nil {
		return err
	}
	return nil
}

func GetProcAddress(handle uintptr, name string) (uintptr, error) {
	if handle == 0 {
		return 0, nil
	}
	addr, err := purego.Dlsym(handle, name)
	if err != nil {
		return 0, err
	}
	return addr, nil
}

// FFI function definitions using the ffi package pattern
var opendalWriterFFI = newFFI(ffiOpts{
	sym:    "opendal_writer",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ffiCall ffiCall) func(*byte) uintptr {
	return func(path *byte) uintptr {
		var ret uintptr
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&path))
		return ret
	}
})

var opendalReaderFFI = newFFI(ffiOpts{
	sym:    "opendal_reader",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ffiCall ffiCall) func(*byte) uintptr {
	return func(path *byte) uintptr {
		var ret uintptr
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&path))
		return ret
	}
})

var opendalWriterFreeFFI = newFFI(ffiOpts{
	sym:    "opendal_writer_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ffiCall ffiCall) func(uintptr) {
	return func(writer uintptr) {
		ffiCall(nil, unsafe.Pointer(&writer))
	}
})

var opendalReaderFreeFFI = newFFI(ffiOpts{
	sym:    "opendal_reader_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ffiCall ffiCall) func(uintptr) {
	return func(reader uintptr) {
		ffiCall(nil, unsafe.Pointer(&reader))
	}
})

var opendalWriterWriteFFI = newFFI(ffiOpts{
	sym:    "opendal_writer_write",
	rType:  &ffi.TypeSint32,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ffiCall ffiCall) func(uintptr, *uint8, uintptr) int32 {
	return func(writer uintptr, data *uint8, length uintptr) int32 {
		var ret int32
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&writer), unsafe.Pointer(&data), unsafe.Pointer(&length))
		return ret
	}
})

var opendalReaderReadFFI = newFFI(ffiOpts{
	sym:    "opendal_reader_read",
	rType:  &ffi.TypeSint32,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ffiCall ffiCall) func(uintptr, *uint8, uintptr) int32 {
	return func(reader uintptr, data *uint8, length uintptr) int32 {
		var ret int32
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&reader), unsafe.Pointer(&data), unsafe.Pointer(&length))
		return ret
	}
})

func init() {
	var err error
	switch runtime.GOOS {
	case "linux":
		_, err = initFFI("opendal/target/debug/libopendal_c.so")
	case "darwin":
		_, err = initFFI("opendal/target/debug/libopendal_c.dylib")
	}
	if err != nil {
		log.Fatal("Failed to load opendal library:", err)
	}
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

// Helper functions that match the original function signatures
func opendalWriter(path *byte) uintptr {
	return opendalWriterFFI.symbol()(path)
}

func opendalReader(path *byte) uintptr {
	return opendalReaderFFI.symbol()(path)
}

func opendalWriterFree(writer uintptr) {
	opendalWriterFreeFFI.symbol()(writer)
}

func opendalReaderFree(reader uintptr) {
	opendalReaderFreeFFI.symbol()(reader)
}

func opendalWriterWrite(writer uintptr, data *uint8, length uintptr) int32 {
	return opendalWriterWriteFFI.symbol()(writer, data, length)
}

func opendalReaderRead(reader uintptr, data *uint8, length uintptr) int32 {
	return opendalReaderReadFFI.symbol()(reader, data, length)
}
