package ffi

import (
	"io"
	"log"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func init() {
	var err error
	switch runtime.GOOS {
	case "linux":
		_, err = initFFI("libc.so.6")
	case "darwin":
		_, err = initFFI("libc.dylib")
	}
	if err != nil {
		log.Fatal("Failed to load libc:", err)
	}
}

type File struct {
	stream uintptr
	name   string
}

func Open(name string) (*File, error) {
	return OpenFile(name, "r")
}

func Create(name string) (*File, error) {
	return OpenFile(name, "w")
}

func CreateFile(name string) (*File, error) {
	return OpenFile(name, "w")
}

func OpenFile(name, mode string) (*File, error) {
	stream, err := libcFopen.symbol()(name, mode)
	if err != nil {
		return nil, err
	}
	if stream == 0 {
		return nil, unix.EINVAL // or some other error
	}

	return &File{
		stream: stream,
		name:   name,
	}, nil
}

// Close implements io.ReadWriteCloser.
func (f *File) Close() error {
	if f.stream == 0 {
		return nil // already closed
	}

	ret := libcFclose.symbol()(f.stream)
	if ret != 0 {
		return unix.EINVAL // failed to close
	}

	f.stream = 0
	return nil
}

// Read implements io.ReadWriteCloser.
func (f *File) Read(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, unix.EBADF // file is closed
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := libcFread.symbol()(unsafe.Pointer(&p[0]), 1, uintptr(len(p)), f.stream)
	return int(count), nil
}

// Write implements io.ReadWriteCloser.
func (f *File) Write(p []byte) (n int, err error) {
	if f.stream == 0 {
		return 0, unix.EBADF // file is closed
	}

	if len(p) == 0 {
		return 0, nil
	}

	count := libcFwrite.symbol()(unsafe.Pointer(&p[0]), 1, uintptr(len(p)), f.stream)
	return int(count), nil
}

// Name returns the name of the file
func (f *File) Name() string {
	return f.name
}

var _ io.ReadWriteCloser = (*File)(nil)

var libcFopen = newFFI(ffiOpts{
	sym:    "fopen",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ffiCall ffiCall) func(string, string) (uintptr, error) {
	return func(name, mode string) (stream uintptr, err error) {
		namePtr, err := unix.BytePtrFromString(name)
		if err != nil {
			return
		}
		modePtr, err := unix.BytePtrFromString(mode)
		if err != nil {
			return
		}
		ffiCall(unsafe.Pointer(&stream), unsafe.Pointer(&namePtr), unsafe.Pointer(&modePtr))
		return
	}
})

var libcFclose = newFFI(ffiOpts{
	sym:    "fclose",
	rType:  &ffi.TypeSint32,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ffiCall ffiCall) func(uintptr) int {
	return func(stream uintptr) int {
		var ret int
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&stream))
		return ret
	}
})

var libcFread = newFFI(ffiOpts{
	sym:    "fread",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ffiCall ffiCall) func(unsafe.Pointer, uintptr, uintptr, uintptr) uintptr {
	return func(ptr unsafe.Pointer, size, nmemb, stream uintptr) uintptr {
		var ret uintptr
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&ptr), unsafe.Pointer(&size), unsafe.Pointer(&nmemb), unsafe.Pointer(&stream))
		return ret
	}
})

var libcFwrite = newFFI(ffiOpts{
	sym:    "fwrite",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ffiCall ffiCall) func(unsafe.Pointer, uintptr, uintptr, uintptr) uintptr {
	return func(ptr unsafe.Pointer, size, nmemb, stream uintptr) uintptr {
		var ret uintptr
		ffiCall(unsafe.Pointer(&ret), unsafe.Pointer(&ptr), unsafe.Pointer(&size), unsafe.Pointer(&nmemb), unsafe.Pointer(&stream))
		return ret
	}
})
