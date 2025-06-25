// This crate is the C binding for the OpenDAL project.
// So it's type node can't meet camel case.
#![allow(non_camel_case_types)]
// This crate is the C binding for the OpenDAL project.
// Nearly all the functions exposed to C FFI are unsafe.
#![allow(clippy::missing_safety_doc)]

mod operator;

pub use operator::*;
