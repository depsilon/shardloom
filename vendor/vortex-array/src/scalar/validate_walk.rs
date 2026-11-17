// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Native scalar validation with ordered borrowed continuations.

use vortex_error::{VortexError, VortexExpect, vortex_err};

use crate::dtype::extension::ExtDTypeRef;
use crate::dtype::{DType, DTypeChildren};

use super::diagnostic::ValueDisplay;
use super::{Scalar, ScalarValue};

type Check = Result<(), Option<VortexError>>;

fn failure<const DETAILED: bool>(make: impl FnOnce() -> VortexError) -> Option<VortexError> {
    DETAILED.then(make)
}

macro_rules! require {
    ($condition:expr, $($message:tt)*) => {
        if !$condition {
            return Err(failure::<DETAILED>(|| vortex_err!($($message)*)));
        }
    };
}

struct Entries<'a> {
    key: &'a DType,
    value: &'a DType,
    remaining: &'a [Option<ScalarValue>],
    index: usize,
}

enum Work<'a> {
    Node(&'a DType, Option<&'a ScalarValue>),
    Entries(Entries<'a>),
}

enum Parent<'a> {
    List {
        dtype: &'a DType,
        remaining: &'a [Option<ScalarValue>],
        index: usize,
        fixed: bool,
    },
    Struct {
        children: DTypeChildren<'a>,
        remaining: &'a [Option<ScalarValue>],
    },
    MapKey {
        entries: Entries<'a>,
        value: Option<&'a ScalarValue>,
    },
    MapValue(Entries<'a>),
    Union {
        dtype: &'a DType,
        tag: u8,
    },
    Extension(&'a ExtDTypeRef, &'a ScalarValue),
    Variant(&'a Scalar),
}

impl Parent<'_> {
    fn has_context(&self) -> bool {
        matches!(
            self,
            Self::List { .. } | Self::MapKey { .. } | Self::MapValue(_) | Self::Union { .. }
        )
    }

    fn context(&self) -> String {
        match self {
            Self::List {
                index,
                fixed: false,
                ..
            } => format!("list element at index {index}"),
            Self::List {
                index, fixed: true, ..
            } => format!("fixed-size list element at index {index}"),
            Self::MapKey { entries, .. } => format!("map key at entry {}", entries.index),
            Self::MapValue(entries) => format!("map value at entry {}", entries.index),
            Self::Union { dtype, tag } => {
                format!("union value for type ID {tag} is invalid for dtype {dtype}")
            }
            Self::Struct { .. } | Self::Extension(..) | Self::Variant(_) => {
                unreachable!("contextless scalar continuation")
            }
        }
    }
}

pub(super) fn check<const DETAILED: bool>(dtype: &DType, value: Option<&ScalarValue>) -> Check {
    let mut pending = Vec::new();
    let result = run::<DETAILED>(dtype, value, &mut pending);
    result.map_err(|error| {
        error.map(|mut error| {
            // Keep the old inner-to-outer context and formatting order. Previously
            // unsupported deep failures cannot expand every ancestor indefinitely.
            let mut contexts = pending.iter().rev().filter(|parent| parent.has_context());
            for parent in contexts.by_ref().take(32) {
                error = error.with_context(parent.context());
            }
            let omitted = contexts.count();
            if omitted != 0 {
                error = error.with_context(format!(
                    "{omitted} outer scalar validation contexts omitted"
                ));
            }
            error
        })
    })
}

fn run<'a, const DETAILED: bool>(
    dtype: &'a DType,
    value: Option<&'a ScalarValue>,
    pending: &mut Vec<Parent<'a>>,
) -> Check {
    let mut work = Work::Node(dtype, value);
    'visit: loop {
        match work {
            Work::Node(dtype, None) => {
                require!(
                    dtype.is_nullable(),
                    "non-nullable dtype {dtype} cannot hold a null value"
                );
            }
            Work::Node(dtype, Some(value)) => {
                let shown = ValueDisplay(value);
                match dtype {
                    DType::Null => {
                        return Err(failure::<DETAILED>(|| {
                            vortex_err!("null dtype cannot hold a non-null value {shown}")
                        }));
                    }
                    DType::Bool(_) => require!(
                        matches!(value, ScalarValue::Bool(_)),
                        "bool dtype expected Bool value, got {shown}"
                    ),
                    DType::Utf8(_) => require!(
                        matches!(value, ScalarValue::Utf8(_)),
                        "utf8 dtype expected Utf8 value, got {shown}"
                    ),
                    DType::Binary(_) => require!(
                        matches!(value, ScalarValue::Binary(_)),
                        "binary dtype expected Binary value, got {shown}"
                    ),
                    DType::Primitive(ptype, _) => {
                        let ScalarValue::Primitive(pvalue) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!(
                                    "primitive dtype {ptype} expected Primitive value, got {shown}"
                                )
                            }));
                        };
                        require!(
                            pvalue.ptype() == *ptype,
                            "primitive dtype {ptype} requires a matching value, got {pvalue}"
                        );
                    }
                    DType::Decimal(decimal, _) => {
                        let ScalarValue::Decimal(dvalue) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!("decimal dtype expected Decimal value, got {shown}")
                            }));
                        };
                        require!(
                            dvalue.fits_in_precision(*decimal),
                            "decimal value {dvalue} does not fit in precision of {decimal}"
                        );
                    }
                    DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                        let fixed = matches!(dtype, DType::FixedSizeList(..));
                        let ScalarValue::Tuple(values) = value else {
                            return Err(failure::<DETAILED>(|| {
                                if fixed {
                                    vortex_err!(
                                        "fixed-size list dtype expected Tuple value, got {shown}"
                                    )
                                } else {
                                    vortex_err!("list dtype expected Tuple value, got {shown}")
                                }
                            }));
                        };
                        if let DType::FixedSizeList(_, size, _) = dtype {
                            require!(
                                values.len() == *size as usize,
                                "fixed-size list scalar has the wrong number of elements\n  left: {}\n right: {}",
                                values.len(),
                                *size as usize
                            );
                        }
                        if let Some((value, remaining)) = values.split_first() {
                            pending.push(Parent::List {
                                dtype: child,
                                remaining,
                                index: 0,
                                fixed,
                            });
                            work = Work::Node(child, value.as_ref());
                            continue;
                        }
                    }
                    DType::Struct(fields, _) => {
                        let ScalarValue::Tuple(values) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!("struct dtype expected Tuple value, got {shown}")
                            }));
                        };
                        require!(
                            values.len() == fields.nfields(),
                            "struct scalar has the wrong number of fields\n  left: {}\n right: {}",
                            values.len(),
                            fields.nfields()
                        );
                        let mut children = dtype.try_child_dtypes();
                        if let (Some(child), Some((value, remaining))) =
                            (children.next(), values.split_first())
                        {
                            let child = child.vortex_expect("field DType must be valid");
                            pending.push(Parent::Struct {
                                children,
                                remaining,
                            });
                            work = Work::Node(child, value.as_ref());
                            continue;
                        }
                    }
                    DType::Map(map, _) => {
                        let ScalarValue::Tuple(entries) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!("map dtype expected Tuple value, got {shown}")
                            }));
                        };
                        // Both fields were resolved before the original entry loop.
                        let key = map
                            .try_key_dtype()
                            .vortex_expect("map key dtype must be valid");
                        let value = map
                            .try_value_dtype()
                            .vortex_expect("map value dtype must be valid");
                        work = Work::Entries(Entries {
                            key,
                            value,
                            remaining: entries,
                            index: 0,
                        });
                        continue;
                    }
                    DType::Union(variants, _) => {
                        let ScalarValue::Union(union) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!("union dtype expected Union value, got {shown}")
                            }));
                        };
                        let tag = union.type_id();
                        let Some(index) = variants.tag_to_child_index(tag) else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!(
                                    "union value has unknown type ID {tag}; expected one of {:?}",
                                    variants.type_ids()
                                )
                            }));
                        };
                        let child = dtype
                            .try_child_dtypes()
                            .nth(index)
                            .vortex_expect("resolved union child index must be valid")
                            .vortex_expect("variant DType must be valid");
                        pending.push(Parent::Union { dtype: child, tag });
                        work = Work::Node(child, union.child_value());
                        continue;
                    }
                    DType::Variant(_) => {
                        let ScalarValue::Variant(inner) = value else {
                            return Err(failure::<DETAILED>(|| {
                                vortex_err!("variant dtype expected Variant value, got {shown}")
                            }));
                        };
                        pending.push(Parent::Variant(inner));
                        work = Work::Node(inner.dtype(), inner.value());
                        continue;
                    }
                    DType::Extension(extension) => {
                        pending.push(Parent::Extension(extension, value));
                        work = Work::Node(extension.storage_dtype(), Some(value));
                        continue;
                    }
                }
            }
            Work::Entries(mut entries) => {
                if let Some((entry, remaining)) = entries.remaining.split_first() {
                    let index = entries.index;
                    let Some(entry) = entry else {
                        return Err(failure::<DETAILED>(|| {
                            vortex_err!("map entry at index {index} cannot be null")
                        }));
                    };
                    let ScalarValue::Tuple(values) = entry else {
                        return Err(failure::<DETAILED>(|| {
                            vortex_err!(
                                "map entry at index {index} expected Tuple value, got {}",
                                ValueDisplay(entry)
                            )
                        }));
                    };
                    require!(
                        values.len() == 2,
                        "map entry at index {index} has the wrong number of values\n  left: {}\n right: 2",
                        values.len()
                    );
                    entries.remaining = remaining;
                    work = Work::Node(entries.key, values[0].as_ref());
                    pending.push(Parent::MapKey {
                        entries,
                        value: values[1].as_ref(),
                    });
                    continue;
                }
            }
        }
        while let Some(parent) = pending.pop() {
            match parent {
                Parent::List {
                    dtype,
                    remaining,
                    index,
                    fixed,
                } => {
                    if let Some((value, remaining)) = remaining.split_first() {
                        pending.push(Parent::List {
                            dtype,
                            remaining,
                            index: index + 1,
                            fixed,
                        });
                        work = Work::Node(dtype, value.as_ref());
                        continue 'visit;
                    }
                }
                Parent::Struct {
                    mut children,
                    remaining,
                } => {
                    if let (Some(child), Some((value, remaining))) =
                        (children.next(), remaining.split_first())
                    {
                        let child = child.vortex_expect("field DType must be valid");
                        pending.push(Parent::Struct {
                            children,
                            remaining,
                        });
                        work = Work::Node(child, value.as_ref());
                        continue 'visit;
                    }
                }
                Parent::MapKey { entries, value } => {
                    work = Work::Node(entries.value, value);
                    pending.push(Parent::MapValue(entries));
                    continue 'visit;
                }
                Parent::MapValue(mut entries) => {
                    entries.index += 1;
                    work = Work::Entries(entries);
                    continue 'visit;
                }
                Parent::Extension(extension, value) => {
                    extension
                        .validate_storage_value(value)
                        .map_err(|error| DETAILED.then_some(error))?;
                }
                Parent::Variant(inner) => require!(
                    !inner.is_null() || matches!(inner.dtype(), DType::Null),
                    "variant nulls must use a nested null scalar, got {}",
                    inner.dtype()
                ),
                Parent::Union { .. } => {}
            }
        }
        return Ok(());
    }
}
