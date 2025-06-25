// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::LazyLock;

use ::opendal as core;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub struct opendal_writer {
    inner: *mut c_void,
    writer: *mut c_void,
}

pub struct opendal_reader {
    inner: *mut c_void,
    reader: *mut c_void,
}

impl opendal_reader {
    pub(crate) fn deref_mut(&mut self) -> &mut core::BlockingReader {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.reader as *mut core::BlockingReader) }
    }
}

impl opendal_writer {
    pub(crate) fn deref_mut(&mut self) -> &mut core::BlockingWriter {
        // Safety: the inner should never be null once constructed
        // The use-after-free is undefined behavior
        unsafe { &mut *(self.writer as *mut core::BlockingWriter) }
    }
}

fn build_operator(
    schema: core::Scheme,
    map: HashMap<String, String>,
) -> core::Result<core::Operator> {
    let mut op = core::Operator::via_iter(schema, map)?.layer(core::layers::RetryLayer::new());
    if !op.info().full_capability().blocking {
        let runtime =
            tokio::runtime::Handle::try_current().unwrap_or_else(|_| RUNTIME.handle().clone());
        let _guard = runtime.enter();
        op = op
            .layer(core::layers::BlockingLayer::create().expect("blocking layer must be created"));
    }
    Ok(op)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_writer(path: *const c_char) -> *mut opendal_writer {
    assert!(!path.is_null());
    let path = unsafe {
        std::ffi::CStr::from_ptr(path)
            .to_str()
            .expect("Invalid UTF-8 string")
    };
    let scheme = core::Scheme::Fs;

    let mut map = HashMap::<String, String>::default();
    map.insert("root".to_string(), "/tmp/opendal/".to_string());
    let op = match build_operator(scheme, map) {
        Ok(op) => op,
        Err(_) => return std::ptr::null_mut(),
    };
    let writer = match op.blocking().writer(path) {
        Ok(w) => w,
        Err(_) => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(opendal_writer {
        inner: Box::into_raw(Box::new(op.blocking())) as _,
        writer: Box::into_raw(Box::new(writer)) as _,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_reader(path: *const c_char) -> *mut opendal_reader {
    assert!(!path.is_null());
    let path = unsafe {
        std::ffi::CStr::from_ptr(path)
            .to_str()
            .expect("Invalid UTF-8 string")
    };
    let scheme = core::Scheme::Fs;

    let mut map = HashMap::<String, String>::default();
    map.insert("root".to_string(), "/tmp/opendal/".to_string());
    let op = match build_operator(scheme, map) {
        Ok(op) => op,
        Err(_) => return std::ptr::null_mut(),
    };
    if !op.blocking().exists(path).unwrap_or(false) {
        return std::ptr::null_mut();
    }
    let reader = match op.blocking().reader(path) {
        Ok(r) => r,
        Err(_) => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(opendal_reader {
        inner: Box::into_raw(Box::new(op.blocking())) as _,
        reader: Box::into_raw(Box::new(reader)) as _,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_writer_free(writer: *mut opendal_writer) {
    assert!(!writer.is_null());
    unsafe {
        drop(Box::from_raw((*writer).writer as *mut core::BlockingWriter));
        drop(Box::from_raw(
            (*writer).inner as *mut core::BlockingOperator,
        ));
        drop(Box::from_raw(writer));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_reader_free(reader: *mut opendal_reader) {
    assert!(!reader.is_null());
    unsafe {
        drop(Box::from_raw((*reader).reader as *mut core::BlockingReader));
        drop(Box::from_raw(
            (*reader).inner as *mut core::BlockingOperator,
        ));
        drop(Box::from_raw(reader));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_writer_write(
    writer: *mut opendal_writer,
    data: *const u8,
    len: usize,
) -> isize {
    assert!(!data.is_null());
    assert!(!writer.is_null());
    let writer = unsafe { &mut *writer };
    let slice = unsafe { std::slice::from_raw_parts(data, len) };
    match writer.deref_mut().write(slice) {
        Ok(_) => len as isize,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendal_reader_read(
    reader: *mut opendal_reader,
    data: *mut u8,
    len: usize,
) -> isize {
    if reader.is_null() || data.is_null() {
        return -1;
    }
    let reader = unsafe { &mut *reader };
    let mut buf = unsafe { std::slice::from_raw_parts_mut(data, len) };
    match reader.deref_mut().read_into(&mut buf, ..len as u64) {
        Ok(size) => size as isize,
        Err(_) => -1,
    }
}
