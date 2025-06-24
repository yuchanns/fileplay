package ffi

import (
	"context"
	"errors"
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

	sym T
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

func BytePtrFromString(s string) (*byte, error) {
	if s == "" {
		return new(byte), nil
	}
	return unix.BytePtrFromString(s)
}

func BytePtrToString(p *byte) string {
	if p == nil {
		return ""
	}
	return unix.BytePtrToString(p)
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
