// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom, 2026-11-08: scoped decimal storage preservation after
// ancestor-aware validation at an admitted Arrow boundary.
// Modified by ShardLoom, 2026-11-09: export Variant chunks before plugin
// canonicalization can hide them behind a self-repeating core wrapper.

//! Plugin layer for moving Arrow extension types in and out of Vortex.
//!
//! Vortex's canonical Arrow conversion (see [`crate::dtype`] and the executor in
//! [`crate::executor`]) handles every non-extension Arrow type and the builtin temporal
//! extensions. The plugins registered here cover the remaining case: **Arrow extension types**.
//!
//! * An [`ArrowExportVTable`] is dispatched purely by the **target Arrow extension Id** —
//!   the plugin is selected when the caller asks for an Arrow [`Field`] carrying matching
//!   `ARROW:extension:name` metadata. The Vortex source dtype/encoding is irrelevant to
//!   dispatch.
//! * An [`ArrowImportVTable`] is dispatched by the **source Arrow extension name** carried
//!   on the incoming [`Field`]. The plugin is responsible for both preserving extension
//!   identity and re-encoding storage if needed (e.g. Arrow `FixedSizeBinary[16]` for UUID
//!   becomes Vortex `FixedSizeList<u8; 16>`).
//!
//! Multiple plugins may register against the same key. They are tried in registration order;
//! each may return [`ArrowExport::Unsupported`] / [`ArrowImport::Unsupported`] to defer to
//! the next.
//!
//! A Vortex extension type that does not have a plugin exports its storage, plus Arrow extension
//! metadata that holds its Vortex ID. [`ArrowSession`] describes this format.

use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::Array as ArrowArray;
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::OffsetSizeTrait;
use arrow_array::RecordBatch;
use arrow_array::RunArray;
use arrow_array::StructArray as ArrowStructArray;
use arrow_array::cast::AsArray;
use arrow_array::make_array;
use arrow_array::types::Int16Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::RunEndIndexType;
use arrow_buffer::NullBuffer;
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Fields;
use arrow_schema::Schema;
use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use tracing::debug;
use tracing::trace;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::FixedSizeListArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::ListViewArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::extension::ExtensionArrayExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::IntegerPType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::dtype::extension::ExtDTypeRef;
use vortex_array::dtype::extension::ExtId;
use vortex_array::dtype::session::DTypeSession;
use vortex_array::dtype::session::ExtDTypeRegistry;
use vortex_array::extension::datetime::AnyTemporal;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_session::ArcSwapMap;
use vortex_session::SessionExt;
use vortex_session::SessionGuard;
use vortex_session::SessionVar;
use vortex_session::registry::Id;

use crate::ArrowExportOptions;
use crate::IntoVortexArray;
use crate::convert::from_arrow_dyn;
use crate::convert::map_from_arrow_parts;
use crate::convert::nulls;
use crate::convert::remove_nulls;
use crate::convert::trim_map_entries;
use crate::convert::trim_offsets;
use crate::dtype::from_arrow_data_type;

#[path = "variant_chunks.rs"]
mod variant_chunks;
use crate::dtype::to_data_type_naive;
use crate::executor::ExportTask;
use crate::executor::run_export;
use crate::run_end_import::extract_run_end_parts;
use crate::run_end_import::finish_run_end;

/// Outcome of a successful call to [`ArrowExportVTable::execute_arrow`].
///
/// Plugins that don't handle the supplied array return [`Unsupported`][Self::Unsupported]
/// with ownership of the input so the session can probe the next plugin or fall back to the
/// canonical path. Errors are propagated through [`VortexResult`].
pub enum ArrowExport {
    /// The plugin does not handle this input; the session may try another plugin.
    Unsupported(ArrayRef),
    /// A successful export.
    Exported(ArrowArrayRef),
}

/// Outcome of a successful call to [`ArrowImportVTable::from_arrow_array`].
///
/// Plugins that don't handle the supplied array return [`Unsupported`][Self::Unsupported]
/// with ownership of the input so the session can probe the next plugin or fall back to the
/// canonical path. Errors are propagated through [`VortexResult`].
pub enum ArrowImport {
    /// The plugin does not handle this input; the session may try another plugin.
    Unsupported(ArrowArrayRef),
    /// A successful import.
    Imported(ArrayRef),
}

/// The Arrow type description of an array being imported by [`ArrowSession::from_arrow_array`].
///
/// Callers holding an Arrow [`Field`] (or [`FieldRef`]) should pass it: its
/// `ARROW:extension:name` metadata is what dispatches the array to a registered
/// [`ArrowImportVTable`].
///
/// An Arrow array can carry a validity (null) buffer regardless of whether its schema declares
/// the field nullable, so when no [`Field`] is in hand the caller passes the desired nullability
/// instead, as a [`bool`] or a [`Nullability`]. An anonymous field is then synthesized from the
/// array's own data type, which means no extension plugin is dispatched for the array itself;
/// fields nested inside container data types still carry their metadata and are routed through
/// their importers.
pub trait IntoArrowField<'a> {
    /// Resolve to the Arrow [`Field`] describing an array of `data_type`.
    fn into_arrow_field(self, data_type: &DataType) -> Cow<'a, Field>;
}

impl<'a> IntoArrowField<'a> for &'a Field {
    fn into_arrow_field(self, _data_type: &DataType) -> Cow<'a, Field> {
        Cow::Borrowed(self)
    }
}

impl<'a> IntoArrowField<'a> for &'a FieldRef {
    fn into_arrow_field(self, _data_type: &DataType) -> Cow<'a, Field> {
        Cow::Borrowed(self.as_ref())
    }
}

impl<'a> IntoArrowField<'a> for bool {
    fn into_arrow_field(self, data_type: &DataType) -> Cow<'a, Field> {
        Cow::Owned(Field::new("", data_type.clone(), self))
    }
}

impl<'a> IntoArrowField<'a> for Nullability {
    fn into_arrow_field(self, data_type: &DataType) -> Cow<'a, Field> {
        self.is_nullable().into_arrow_field(data_type)
    }
}

/// Plugin layer for exporting a Vortex array to an Arrow extension type.
///
/// This is purely an implementation trait, its methods should not be called directly. Instead,
/// use the methods on [`ArrowSession`].
pub trait ArrowExportVTable: 'static + Send + Sync + Debug {
    /// The Arrow extension ID this plugin produces.
    fn arrow_ext_id(&self) -> Id;

    /// The Vortex array or extension ID this plugin maps from. Used only for inference by
    /// [`ArrowSession::to_arrow_field`] / [`ArrowSession::to_arrow_schema`]; never as a
    /// dispatch key for [`execute_arrow`][Self::execute_arrow].
    fn vortex_id(&self) -> Id;

    /// Build the Arrow [`Field`] this plugin produces for the given Vortex extension
    /// `dtype`. Used during schema inference.
    fn to_arrow_field(
        &self,
        name: &str,
        dtype: &DType,
        session: &ArrowSession,
    ) -> VortexResult<Option<Field>>;

    /// Convert a Vortex array into an Arrow array shaped to `target`.
    ///
    /// Returns ownership of `array` via [`ArrowExport::Unsupported`] when the plugin cannot
    /// handle the input. Plugins that export child or storage arrays must pass `options` on to
    /// those exports.
    fn execute_arrow(
        &self,
        array: ArrayRef,
        target: &Field,
        options: &ArrowExportOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowExport>;
}

/// Plugin layer for importing an Arrow extension-typed array into a Vortex array.
///
/// Plugins are dispatched by `arrow_ext_id`. The session peels Arrow dictionaries before it
/// dispatches, so a plugin never sees a [`DataType::Dictionary`] field or array. It sees the value
/// field, with the dictionary field's metadata, and the values array. The session then wraps the
/// plugin's result in a [`DictArray`] with the dictionary's codes.
///
/// Arrow stores dictionary values as a bare [`DataType`], so their extension metadata lives on
/// the outer [`Field`]. Peeling the dictionary lets the importer see the value type described by
/// that metadata. [`DataType::RunEndEncoded`] has a separate values field that can carry its own
/// extension metadata, so its values use the normal nested-field import path.
///
/// This is purely an implementation trait, its methods should not be called directly. Instead,
/// use the methods on [`ArrowSession`].
pub trait ArrowImportVTable: 'static + Send + Sync + Debug {
    /// The Arrow extension name this plugin handles.
    fn arrow_ext_id(&self) -> Id;

    /// Build the Vortex [`DType`] that corresponds to `field` (which carries this plugin's
    /// Arrow extension metadata).
    ///
    /// `session` is provided so plugins can resolve nested or storage fields through the
    /// plugin-aware conversion (e.g. [`ArrowSession::from_arrow_datatype`]) instead of the
    /// naive Arrow → Vortex mapping.
    #[allow(clippy::wrong_self_convention)]
    fn from_arrow_field(
        &self,
        field: &Field,
        session: &ArrowSession,
    ) -> VortexResult<Option<DType>>;

    /// Convert an Arrow array into a Vortex array of `dtype`.
    ///
    /// Returns ownership of `array` via [`ArrowImport::Unsupported`] when the plugin cannot
    /// handle the input.
    ///
    /// `session` is provided so plugins can convert storage or nested arrays through the
    /// session (e.g. [`ArrowSession::from_arrow_array`], passing a nullability rather than a
    /// [`Field`] so the plugin is not dispatched again).
    #[allow(clippy::wrong_self_convention)]
    fn from_arrow_array(
        &self,
        array: ArrowArrayRef,
        field: &Field,
        dtype: &DType,
        session: &ArrowSession,
    ) -> VortexResult<ArrowImport>;
}

pub type ArrowExportVTableRef = Arc<dyn ArrowExportVTable>;
pub type ArrowImportVTableRef = Arc<dyn ArrowImportVTable>;

/// Registry of Arrow exporters, keyed by target Arrow extension [`Id`].
type ArrowExporterRegistry = ArcSwapMap<Id, Arc<[ArrowExportVTableRef]>>;
/// Registry of Arrow exporters, keyed by source Vortex extension [`ExtId`].
type VortexExporterRegistry = ArcSwapMap<ExtId, Arc<[ArrowExportVTableRef]>>;
/// Registry of Arrow importers, keyed by source Arrow extension [`Id`].
type ArrowImporterRegistry = ArcSwapMap<Id, Arc<[ArrowImportVTableRef]>>;

/// Session-scoped registry of Arrow extension plugins.
///
/// Exporters are stored in two indices: one keyed by Arrow extension Id (used for
/// `execute_arrow` dispatch) and one keyed by Vortex extension Id (used **only** by
/// `to_arrow_field` / `to_arrow_schema` inference, when callers need to translate a Vortex
/// extension `DType` into an Arrow `Field` with no target schema in hand). Importers are
/// keyed by Arrow extension name. Extension plugins are registered by the crate that owns the
/// extension type (for example `vortex-uuid`); temporal extensions are handled by the canonical
/// Arrow ↔ Vortex path and do not need plugins.
///
/// An extension type that does not have an exporter exports its storage field, with two Arrow
/// metadata keys added: `ARROW:extension:name` holds its Vortex ID, and `ARROW:extension:metadata`
/// holds its serialized metadata in standard padded base64. The metadata is base64 because Arrow
/// field metadata must be UTF-8, which is also how Parquet stores `ARROW:schema`. DataFusion
/// compares these strings when it merges file schemas, so [`ExtDTypeRef::serialize_metadata`] must
/// give equal bytes for equal dtypes. The export fails if the storage field already has an Arrow
/// extension name, because one field cannot have two.
///
/// A caller can also export to such a field directly. If no exporter accepts the target, but its
/// extension name is the Vortex ID of the array, the array exports its storage. This works for
/// every extension type, so a caller can export a timestamp column as `Int64` without losing its
/// type. However, the target must have the same metadata as the array, because different metadata
/// would give the values a different meaning, such as a different time unit for a timestamp. To
/// check this, a session that knows the Vortex ID rebuilds the dtype from the target and compares
/// the dtypes, and any other session compares the metadata bytes.
///
/// On import, the importers run first. If none of them accepts the field, but its extension name is
/// a Vortex ID that the session knows (see [`initialize`]), the field imports as that extension
/// type over its storage. In that case, the import fails if the type rejects the metadata or the
/// storage. A field with any other extension name imports as plain storage.
///
/// [`initialize`]: crate::initialize
#[derive(Clone, Debug, Default)]
pub struct ArrowSession {
    exporters: ArrowExporterRegistry,
    exporters_by_vortex: VortexExporterRegistry,
    importers: ArrowImporterRegistry,
    /// The extension dtypes that the session can look up by Vortex ID. [`Self::new`] shares the
    /// registry of a [`DTypeSession`], so dtypes that are registered there later are also found.
    /// The default session has none.
    ext_dtypes: ExtDTypeRegistry,
    preserve_decimal_storage: bool,
}

impl ArrowSession {
    /// Clone this import session while preserving decimal widths and buffers.
    ///
    /// The caller must validate decimal precision for every reachable value,
    /// respecting ancestor validity and selection, before observing the result.
    /// Metadata, buffer layout and row validity remain checked. This avoids
    /// inspecting unreachable values or narrowing shared storage. The original
    /// session and the default canonical decimal import policy are unchanged.
    pub fn with_preserved_decimal_storage(&self) -> Self {
        Self {
            preserve_decimal_storage: true,
            ..self.clone()
        }
    }

    /// Create a session that can look up the extension dtypes of `dtype_session` by Vortex ID.
    pub(crate) fn new(dtype_session: &DTypeSession) -> Self {
        Self {
            ext_dtypes: dtype_session.registry().clone(),
            ..Self::default()
        }
    }

    /// Register an [`ArrowExportVTable`] under its target Arrow extension Id (for dispatch)
    /// and its source Vortex extension Id (for schema inference).
    pub fn register_exporter(&self, exporter: ArrowExportVTableRef) {
        self.exporters.push(
            exporter.arrow_ext_id(),
            ArrowExportVTableRef::clone(&exporter),
        );
        self.exporters_by_vortex
            .push(exporter.vortex_id(), exporter);
    }

    /// Register an [`ArrowImportVTable`] under its source Arrow extension name.
    pub fn register_importer(&self, importer: ArrowImportVTableRef) {
        self.importers.push(importer.arrow_ext_id(), importer);
    }

    fn exporters(&self, id: &Id) -> Arc<[ArrowExportVTableRef]> {
        self.exporters.get(id).unwrap_or_else(|| Arc::from([]))
    }

    fn exporters_by_vortex(&self, id: &Id) -> Arc<[ArrowExportVTableRef]> {
        self.exporters_by_vortex
            .get(id)
            .unwrap_or_else(|| Arc::from([]))
    }

    fn importers(&self, id: &Id) -> Arc<[ArrowImportVTableRef]> {
        self.importers.get(id).unwrap_or_else(|| Arc::from([]))
    }

    /// Build the Arrow [`Field`] for a Vortex [`DType`].
    ///
    /// For [`DType::Extension`]s, plugins registered against the extension's `Id`
    /// are tried in registration order; the first plugin to return `Some(field)` wins.
    ///
    /// An extension type without a plugin exports its storage field, plus Arrow extension metadata
    /// that holds its Vortex ID (see [`ArrowSession`]).
    pub fn to_arrow_field(&self, name: &str, dtype: &DType) -> VortexResult<Field> {
        enum ToFieldStep {
            Completed(Field),
            Descend {
                frame: ToFieldFrame,
                name: String,
                dtype: DType,
            },
        }

        enum ToFieldFrame {
            List {
                name: String,
                nullable: bool,
            },
            FixedSizeList {
                name: String,
                elem_size: i32,
                nullable: bool,
            },
            MapKey {
                name: String,
                value_dtype: DType,
                keys_sorted: bool,
                nullable: bool,
            },
            MapValue {
                name: String,
                key_field: Field,
                keys_sorted: bool,
                nullable: bool,
            },
            Struct {
                name: String,
                fields: StructFields,
                next_idx: usize,
                built: Vec<Field>,
                nullable: bool,
            },
            WrapExtension {
                ext_dtype: ExtDTypeRef,
            },
        }

        let start_node = |node_name: &str, node_dtype: &DType| -> VortexResult<ToFieldStep> {
            match node_dtype {
                DType::List(elem_dtype, nullability) => Ok(ToFieldStep::Descend {
                    frame: ToFieldFrame::List {
                        name: node_name.to_owned(),
                        nullable: nullability.is_nullable(),
                    },
                    name: Field::LIST_FIELD_DEFAULT_NAME.to_owned(),
                    dtype: elem_dtype.as_ref().clone(),
                }),
                DType::FixedSizeList(elem_dtype, elem_size, nullability) => {
                    let size: i32 = (*elem_size).try_into()?;
                    Ok(ToFieldStep::Descend {
                        frame: ToFieldFrame::FixedSizeList {
                            name: node_name.to_owned(),
                            elem_size: size,
                            nullable: nullability.is_nullable(),
                        },
                        name: Field::LIST_FIELD_DEFAULT_NAME.to_owned(),
                        dtype: elem_dtype.as_ref().clone(),
                    })
                }
                DType::Map(map_dtype, nullability) => Ok(ToFieldStep::Descend {
                    frame: ToFieldFrame::MapKey {
                        name: node_name.to_owned(),
                        value_dtype: map_dtype.value_dtype(),
                        keys_sorted: map_dtype.keys_sorted(),
                        nullable: nullability.is_nullable(),
                    },
                    name: "key".to_owned(),
                    dtype: map_dtype.key_dtype(),
                }),
                DType::Struct(fields, nullability) => {
                    if let (Some(first_name), Some(first_dtype)) =
                        (fields.names().get(0), fields.field_by_index(0))
                    {
                        let built = Vec::with_capacity(fields.names().len());
                        Ok(ToFieldStep::Descend {
                            frame: ToFieldFrame::Struct {
                                name: node_name.to_owned(),
                                fields: fields.clone(),
                                next_idx: 0,
                                built,
                                nullable: nullability.is_nullable(),
                            },
                            name: first_name.as_ref().to_owned(),
                            dtype: first_dtype,
                        })
                    } else {
                        Ok(ToFieldStep::Completed(Field::new_struct(
                            node_name,
                            Fields::empty(),
                            nullability.is_nullable(),
                        )))
                    }
                }
                DType::Extension(ext) if !ext.is::<AnyTemporal>() => {
                    match self.to_arrow_field_with_exporter(node_name, ext)? {
                        Some(field) => Ok(ToFieldStep::Completed(field)),
                        None => Ok(ToFieldStep::Descend {
                            frame: ToFieldFrame::WrapExtension {
                                ext_dtype: ext.clone(),
                            },
                            name: node_name.to_owned(),
                            dtype: ext.storage_dtype().clone(),
                        }),
                    }
                }
                DType::Variant(_) => Ok(ToFieldStep::Completed(variant_arrow_field(
                    node_name,
                    node_dtype.is_nullable(),
                ))),
                _ => Ok(ToFieldStep::Completed(Field::new(
                    node_name,
                    to_data_type_naive(node_dtype)?,
                    node_dtype.is_nullable(),
                ))),
            }
        };

        let mut stack: Vec<ToFieldFrame> = Vec::new();
        let mut cur_name = Cow::Borrowed(name);
        let mut cur_dtype = Cow::Borrowed(dtype);

        loop {
            let mut step = start_node(cur_name.as_ref(), cur_dtype.as_ref())?;
            loop {
                match step {
                    ToFieldStep::Descend {
                        frame,
                        name: child_name,
                        dtype: child_dtype,
                    } => {
                        stack.push(frame);
                        cur_name = Cow::Owned(child_name);
                        cur_dtype = Cow::Owned(child_dtype);
                        break;
                    }
                    ToFieldStep::Completed(completed) => match stack.pop() {
                        None => return Ok(completed),
                        Some(ToFieldFrame::List {
                            name: wrapper_name,
                            nullable,
                        }) => {
                            step = ToFieldStep::Completed(Field::new_list(
                                wrapper_name,
                                completed,
                                nullable,
                            ));
                        }
                        Some(ToFieldFrame::FixedSizeList {
                            name: wrapper_name,
                            elem_size,
                            nullable,
                        }) => {
                            step = ToFieldStep::Completed(Field::new_fixed_size_list(
                                wrapper_name,
                                completed,
                                elem_size,
                                nullable,
                            ));
                        }
                        Some(ToFieldFrame::MapKey {
                            name: wrapper_name,
                            value_dtype,
                            keys_sorted,
                            nullable,
                        }) => {
                            step = ToFieldStep::Descend {
                                frame: ToFieldFrame::MapValue {
                                    name: wrapper_name,
                                    key_field: completed,
                                    keys_sorted,
                                    nullable,
                                },
                                name: "value".to_owned(),
                                dtype: value_dtype,
                            };
                        }
                        Some(ToFieldFrame::MapValue {
                            name: wrapper_name,
                            key_field,
                            keys_sorted,
                            nullable,
                        }) => {
                            let entries = Field::new_struct(
                                "entries",
                                Fields::from(vec![key_field, completed]),
                                false,
                            );
                            step = ToFieldStep::Completed(Field::new(
                                wrapper_name,
                                DataType::Map(Arc::new(entries), keys_sorted),
                                nullable,
                            ));
                        }
                        Some(ToFieldFrame::Struct {
                            name: wrapper_name,
                            fields,
                            next_idx,
                            mut built,
                            nullable,
                        }) => {
                            built.push(completed);
                            let next = next_idx + 1;
                            let next_entry = fields
                                .names()
                                .get(next)
                                .zip(fields.field_by_index(next))
                                .map(|(next_name, next_dtype)| {
                                    (next_name.as_ref().to_owned(), next_dtype)
                                });
                            if let Some((next_name, next_dtype)) = next_entry {
                                step = ToFieldStep::Descend {
                                    frame: ToFieldFrame::Struct {
                                        name: wrapper_name,
                                        fields,
                                        next_idx: next,
                                        built,
                                        nullable,
                                    },
                                    name: next_name,
                                    dtype: next_dtype,
                                };
                            } else {
                                step = ToFieldStep::Completed(Field::new_struct(
                                    wrapper_name,
                                    Fields::from(built),
                                    nullable,
                                ));
                            }
                        }
                        Some(ToFieldFrame::WrapExtension { ext_dtype }) => {
                            let mut field = completed;
                            if let Some(storage_name) = field.extension_type_name() {
                                vortex_bail!(
                                    "the Arrow storage of {ext_dtype} must not be an Arrow extension type, got {storage_name}"
                                );
                            }
                            let metadata = BASE64_STANDARD.encode(ext_dtype.serialize_metadata()?);
                            field.metadata_mut().extend([
                                (
                                    EXTENSION_TYPE_NAME_KEY.to_owned(),
                                    ext_dtype.id().to_string(),
                                ),
                                (EXTENSION_TYPE_METADATA_KEY.to_owned(), metadata),
                            ]);
                            step = ToFieldStep::Completed(field);
                        }
                    },
                }
            }
        }
    }

    /// Build the Arrow [`Schema`] for a Vortex top-level [`DType::Struct`], dispatching
    /// extension fields through registered export plugins for inference. Nested
    /// extensions are preserved via [`Self::to_arrow_field`].
    pub fn to_arrow_schema(&self, dtype: &DType) -> VortexResult<Schema> {
        let DType::Struct(struct_dtype, _) = dtype else {
            vortex_bail!("to_arrow_schema requires a top-level struct dtype, got {dtype}");
        };
        let mut fields = Vec::with_capacity(struct_dtype.names().len());
        for (name, field_dtype) in struct_dtype.names().iter().zip(struct_dtype.fields()) {
            fields.push(self.to_arrow_field(name.as_ref(), &field_dtype)?);
        }
        Ok(Schema::new(fields))
    }

    /// Returns the Arrow [`DataType`] that best corresponds to the given Vortex [`DType`],
    /// dispatching [`DType::Extension`]s through registered export plugins.
    ///
    /// A [`DataType`] has no field metadata, so this fails for the same types that
    /// [`Self::default_arrow_export_field`] rejects. Use [`Self::to_arrow_field`] to keep the
    /// extension type.
    pub fn to_arrow_datatype(&self, dtype: &DType) -> VortexResult<DataType> {
        Ok(self.default_arrow_export_field(dtype)?.data_type().clone())
    }

    /// Returns the Arrow [`Field`] that an export without a target field uses.
    ///
    /// This is [`Self::to_arrow_field`] with an empty name, except that it fails for a _top-level_
    /// extension type that does not have a plugin. The caller of an export without a target keeps
    /// only the Arrow array or its [`DataType`], not this field. So the extension metadata on the
    /// field would be lost, and with it the extension type. A nested extension type is fine,
    /// because its child [`Field`] keeps the metadata. A top-level extension type with a plugin
    /// still exports, but it loses its extension name, as it did before ([#11286]).
    ///
    /// [#11286]: https://github.com/vortex-data/vortex/issues/11286
    pub fn default_arrow_export_field(&self, dtype: &DType) -> VortexResult<Field> {
        match dtype {
            DType::Extension(ext) if !ext.is::<AnyTemporal>() => {
                self.to_arrow_field_with_exporter("", ext)?.ok_or_else(|| {
                    vortex_err!(
                        "an Arrow export without a target field needs an extension type with an Arrow plugin, got {ext}"
                    )
                })
            }
            _ => self.to_arrow_field("", dtype),
        }
    }

    /// Returns the Arrow [`Field`] from the first exporter that accepts `ext_dtype`, or `None` if
    /// no exporter accepts it.
    fn to_arrow_field_with_exporter(
        &self,
        name: &str,
        ext_dtype: &ExtDTypeRef,
    ) -> VortexResult<Option<Field>> {
        let dtype = DType::Extension(ext_dtype.clone());

        for plugin in self.exporters_by_vortex(&ext_dtype.id()).iter() {
            if let Some(field) = plugin.to_arrow_field(name, &dtype, self)? {
                return Ok(Some(field));
            }
        }

        Ok(None)
    }

    /// Rebuild the extension dtype whose Vortex ID is the extension name of `field`, with
    /// `storage_dtype` as its storage.
    ///
    /// Returns `None` if the session does not know that Vortex ID, and an error if the metadata of
    /// `field` is not valid for that dtype.
    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    fn rebuild_ext_dtype_by_vortex_id(
        &self,
        field: &Field,
        storage_dtype: &DType,
    ) -> VortexResult<Option<ExtDTypeRef>> {
        let Some(dtype_plugin) = field
            .extension_type_name()
            .and_then(|name| self.ext_dtypes.get(&Id::new(name)))
        else {
            return Ok(None);
        };

        dtype_plugin
            .deserialize(&decode_ext_metadata(field)?, storage_dtype.clone())
            .map(Some)
    }

    /// Build the Vortex [`DType`] for an Arrow [`Field`].
    ///
    /// A [`DataType::Dictionary`] field converts as its value field with the same metadata.
    ///
    /// Plugins registered against the field's Arrow extension name are tried in
    /// registration order; the first plugin to return `Some(dtype)` wins. If none
    /// match (or all return `None`), the builtin `arrow.parquet.variant` extension maps
    /// to [`DType::Variant`], and any other field converts through
    /// [`Self::from_arrow_datatype`] so extension metadata on nested element/struct
    /// fields is preserved.
    ///
    /// If the extension name of `field` is a Vortex ID that the session knows, the result is that
    /// extension type over the converted storage (see [`ArrowSession`]).
    pub fn from_arrow_field(&self, field: &Field) -> VortexResult<DType> {
        self.run_import_dtype(ImportDTypeTask::Field(field.clone()))
    }

    /// Build the Vortex [`DType`] for an Arrow [`DataType`].
    ///
    /// Traverses container types ([`DataType::List`] family, [`DataType::FixedSizeList`],
    /// [`DataType::Struct`], [`DataType::Map`], [`DataType::RunEndEncoded`]) iteratively via
    /// the field importer so extension metadata on nested fields dispatches through registered
    /// import plugins. Leaf types use the canonical Arrow → Vortex mapping.
    ///
    /// [`DataType::Dictionary`] is a partial exception: Arrow models its values as a bare
    /// [`DataType`] rather than a [`Field`], so the values themselves cannot carry an extension
    /// name. Fields nested *inside* that data type still do, and are dispatched normally.
    pub fn from_arrow_datatype(
        &self,
        data_type: &DataType,
        nullability: Nullability,
    ) -> VortexResult<DType> {
        self.run_import_dtype(ImportDTypeTask::DataType {
            data_type: data_type.clone(),
            nullability,
        })
    }

    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    fn prepare_import_field(&self, field: Field) -> VortexResult<ImportFieldStep> {
        let mut current = field;
        while let DataType::Dictionary(_, value_type) = current.data_type() {
            let next_dt = value_type.as_ref().clone();
            current = current.with_data_type(next_dt);
        }

        if let Some(name) = current.metadata().get(EXTENSION_TYPE_NAME_KEY) {
            for plugin in self.importers(&Id::new(name)).iter() {
                if let Some(dtype) = plugin.from_arrow_field(&current, self)? {
                    return Ok(ImportFieldStep::Done(dtype));
                }
            }
            // Parquet Variant is understood even without a registered importer plugin.
            if name == "arrow.parquet.variant" {
                return Ok(ImportFieldStep::Done(DType::Variant(
                    current.is_nullable().into(),
                )));
            }
        }

        let wrap_field = current
            .extension_type_name()
            .is_some()
            .then(|| current.clone());
        let nullability = current.is_nullable().into();
        Ok(ImportFieldStep::DataType {
            wrap_field,
            data_type: current.data_type().clone(),
            nullability,
        })
    }

    fn run_import_dtype(&self, initial: ImportDTypeTask) -> VortexResult<DType> {
        let mut stack: Vec<ImportDTypeFrame> = Vec::new();
        let mut current = initial;

        loop {
            let mut step = match current {
                ImportDTypeTask::Field(field) => match self.prepare_import_field(field)? {
                    ImportFieldStep::Done(dtype) => ImportDTypeStep::Completed(dtype),
                    ImportFieldStep::DataType {
                        wrap_field,
                        data_type,
                        nullability,
                    } => {
                        if let Some(ext_field) = wrap_field {
                            stack.push(ImportDTypeFrame::WrapExtension { field: ext_field });
                        }
                        step_import_data_type(&data_type, nullability)?
                    }
                },
                ImportDTypeTask::DataType {
                    data_type,
                    nullability,
                } => step_import_data_type(&data_type, nullability)?,
            };

            loop {
                match step {
                    ImportDTypeStep::TailField(field) => {
                        current = ImportDTypeTask::Field(field);
                        break;
                    }
                    ImportDTypeStep::Descend { frame, task } => {
                        stack.push(frame);
                        current = task;
                        break;
                    }
                    ImportDTypeStep::Completed(completed) => match stack.pop() {
                        None => return Ok(completed),
                        Some(frame) => {
                            step = self.advance_import_dtype_frame(frame, completed)?;
                        }
                    },
                }
            }
        }
    }

    fn advance_import_dtype_frame(
        &self,
        frame: ImportDTypeFrame,
        completed: DType,
    ) -> VortexResult<ImportDTypeStep> {
        match frame {
            ImportDTypeFrame::WrapExtension { field } => {
                let dtype = match self.rebuild_ext_dtype_by_vortex_id(&field, &completed)? {
                    Some(ext_dtype) => DType::Extension(ext_dtype),
                    None => completed,
                };
                Ok(ImportDTypeStep::Completed(dtype))
            }
            ImportDTypeFrame::List { nullability } => Ok(ImportDTypeStep::Completed(DType::List(
                Arc::new(completed),
                nullability,
            ))),
            ImportDTypeFrame::FixedSizeList { size, nullability } => Ok(
                ImportDTypeStep::Completed(DType::FixedSizeList(Arc::new(completed), size, nullability)),
            ),
            ImportDTypeFrame::MapKey {
                value_field,
                keys_sorted,
                nullability,
            } => Ok(ImportDTypeStep::Descend {
                frame: ImportDTypeFrame::MapValue {
                    key_dtype: completed,
                    keys_sorted,
                    nullability,
                },
                task: ImportDTypeTask::Field(value_field.as_ref().clone()),
            }),
            ImportDTypeFrame::MapValue {
                key_dtype,
                keys_sorted,
                nullability,
            } => DType::map(key_dtype, completed, keys_sorted, nullability)
                .map(ImportDTypeStep::Completed),
            ImportDTypeFrame::Struct {
                fields,
                next_idx,
                mut built,
                nullability,
            } => {
                let name = FieldName::from(fields[next_idx].name().as_str());
                built.push((name, completed));
                let next = next_idx + 1;
                let next_field = fields.get(next).map(|fld| fld.as_ref().clone());
                if let Some(next_field) = next_field {
                    Ok(ImportDTypeStep::Descend {
                        frame: ImportDTypeFrame::Struct {
                            fields,
                            next_idx: next,
                            built,
                            nullability,
                        },
                        task: ImportDTypeTask::Field(next_field),
                    })
                } else {
                    Ok(ImportDTypeStep::Completed(DType::Struct(
                        built.into_iter().collect(),
                        nullability,
                    )))
                }
            }
        }
    }

    /// Build Vortex [`StructFields`] for Arrow [`Fields`], dispatching each field through
    /// [`Self::from_arrow_field`].
    pub fn from_arrow_fields(&self, fields: &Fields) -> VortexResult<StructFields> {
        let mut out = Vec::with_capacity(fields.len());
        for arrow_field in fields.iter() {
            let dtype = self.from_arrow_field(arrow_field)?;
            out.push((FieldName::from(arrow_field.name().as_str()), dtype));
        }
        Ok(out.into_iter().collect())
    }

    /// Build the Vortex [`DType`] for an Arrow [`Schema`], dispatching extension fields
    /// through registered import plugins. The result is a top-level non-nullable struct
    /// matching the schema's fields.
    pub fn from_arrow_schema(&self, schema: &Schema) -> VortexResult<DType> {
        Ok(DType::Struct(
            self.from_arrow_fields(schema.fields())?,
            Nullability::NonNullable,
        ))
    }

    /// Decode an Arrow [`RecordBatch`] into a Vortex struct array, dispatching each
    /// extension column through its registered import plugin.
    ///
    /// `schema` is the authoritative Arrow schema used for dispatch — the columns are
    /// consumed positionally. Pass an external schema (rather than relying on
    /// `batch.schema()`) when upstream DataFusion plumbing may have stripped Field-level
    /// extension metadata from the runtime RecordBatch.
    pub fn from_arrow_record_batch(
        &self,
        batch: RecordBatch,
        schema: &Schema,
    ) -> VortexResult<ArrayRef> {
        vortex_ensure_eq!(batch.num_columns(), schema.fields().len());
        let length = batch.num_rows();
        let names = FieldNames::from_iter(
            schema
                .fields()
                .iter()
                .map(|f| FieldName::from(f.name().as_str())),
        );
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (col, field) in batch.columns().iter().zip(schema.fields().iter()) {
            columns.push(self.from_arrow_array_inner(ArrowArrayRef::clone(col), field)?);
        }
        Ok(StructArray::try_new(names, columns, length, Validity::NonNullable)?.into_array())
    }

    /// Execute a Vortex array into an Arrow array.
    ///
    /// If `target` carries an `ARROW:extension:name`, the plugin registry is probed for one that
    /// can support executing to the target extension type. If no plugin accepts it, but the
    /// extension name is the Vortex ID of the array, the array exports its storage (see
    /// [`ArrowSession`]).
    ///
    /// With `target = None`, the export uses the field from [`Self::default_arrow_export_field`],
    /// so it fails for the same types. If that field has an Arrow extension name, the plugins
    /// export the array to it, as above. Otherwise, the array exports to its preferred Arrow type,
    /// and each nested extension type exports through its plugin, or as its storage plus Arrow
    /// extension metadata.
    ///
    /// Uses the default [`ArrowExportOptions`]; see [`exporter`](Self::exporter) to
    /// change them.
    pub fn execute_arrow(
        &self,
        array: ArrayRef,
        target: Option<&Field>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowArrayRef> {
        self.exporter(&ArrowExportOptions::default())
            .execute_arrow(array, target, ctx)
    }

    /// Return an exporter that applies `options` to its exports.
    ///
    /// ```
    /// use arrow_array::Array;
    /// use vortex_array::IntoArray;
    /// use vortex_array::VortexSessionExecute;
    /// use vortex_session::VortexSession;
    /// use vortex_array::arrays::VarBinViewArray;
    /// use vortex_arrow::ArrowExportOptions;
    /// use vortex_arrow::ArrowSessionExt;
    /// use vortex_arrow::CompactBuffers;
    ///
    /// let session = VortexSession::empty();
    /// let array = VarBinViewArray::from_iter_str(["a", "b"]).into_array();
    /// let options = ArrowExportOptions::default().with(CompactBuffers(false));
    /// let arrow = session.arrow().exporter(&options).execute_arrow(
    ///     array,
    ///     None,
    ///     &mut session.create_execution_ctx(),
    /// )?;
    /// assert_eq!(arrow.len(), 2);
    /// # Ok::<(), vortex_error::VortexError>(())
    /// ```
    pub fn exporter<'a>(&'a self, options: &'a ArrowExportOptions) -> ArrowExporter<'a> {
        ArrowExporter {
            session: self,
            options,
        }
    }

    /// Decode an Arrow array into a Vortex array.
    ///
    /// `field` describes the Arrow type the array is imported from: pass an Arrow [`Field`] when
    /// one is available, or just the desired nullability (`true` / `false` /
    /// [`Nullability`]) to synthesize an anonymous field from the array's own data type. See
    /// [`IntoArrowField`] for the trade-off between the two.
    ///
    /// Routes through the registered import plugin if `field` carries an Arrow extension
    /// name we recognize, probing each plugin in registration order until one handles the
    /// input or all return [`ArrowImport::Unsupported`]. Otherwise recurses into container
    /// arrays ([`arrow_array::StructArray`], [`arrow_array::GenericListArray`],
    /// [`arrow_array::FixedSizeListArray`], [`arrow_array::GenericListViewArray`]) so
    /// extension fields nested inside containers reach their importers; leaf types fall
    /// through to the canonical Arrow → Vortex array conversion.
    ///
    /// If the extension name of `field` is a Vortex ID that the session knows, the import wraps the
    /// converted array in that extension type (see [`ArrowSession`]).
    ///
    /// A dictionary array imports as a [`DictArray`]. Its values import as described above, with
    /// `field`'s metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the field (or requested nullability) is non-nullable but the array
    /// physically contains nulls, or if the Arrow data type is unsupported.
    pub fn from_arrow_array<'a>(
        &self,
        array: ArrowArrayRef,
        field: impl IntoArrowField<'a>,
    ) -> VortexResult<ArrayRef> {
        let field = field.into_arrow_field(array.data_type());
        self.from_arrow_array_inner(array, field.as_ref())
    }

    /// [`Self::from_arrow_array`] with the Arrow [`Field`] already resolved: peel a dictionary,
    /// probe the import plugins registered for the field's extension name, then fall back to the
    /// canonical conversion. If the extension name of the field is a Vortex ID that the session
    /// knows, the result is wrapped in that extension type. Also the recursion point for nested
    /// fields, which already have a [`Field`].
    #[allow(clippy::wrong_self_convention)]
    fn from_arrow_array_inner(
        &self,
        array: ArrowArrayRef,
        field: &Field,
    ) -> VortexResult<ArrayRef> {
        let mut stack: Vec<ImportArrayFrame> = Vec::new();
        let mut cur_array = array;
        let mut cur_field = Cow::Borrowed(field);

        loop {
            while let DataType::Dictionary(..) = cur_field.data_type() {
                let dict = cur_array.as_any_dictionary_opt().ok_or_else(|| {
                    vortex_err!(
                        "expected an Arrow DictionaryArray, got {}",
                        cur_array.data_type()
                    )
                })?;
                let codes = dict.keys();
                let codes = from_arrow_dyn(codes, codes.is_nullable())?;
                stack.push(ImportArrayFrame::Dict { codes });
                let values = ArrowArrayRef::clone(dict.values());
                let values_field = cur_field
                    .into_owned()
                    .with_data_type(values.data_type().clone());
                cur_array = values;
                cur_field = Cow::Owned(values_field);
            }

            let mut step = if let Some(imported) =
                self.try_import_with_plugins(&mut cur_array, cur_field.as_ref())?
            {
                ImportArrayStep::Completed(imported)
            } else {
                if cur_field.extension_type_name().is_some() {
                    stack.push(ImportArrayFrame::WrapExtension {
                        field: cur_field.as_ref().clone(),
                    });
                }
                self.step_from_arrow_array_canonical(cur_array.as_ref(), cur_field.as_ref())?
            };

            loop {
                match step {
                    ImportArrayStep::Descend { frame, array, field } => {
                        stack.push(frame);
                        cur_array = array;
                        cur_field = Cow::Owned(field);
                        break;
                    }
                    ImportArrayStep::Completed(completed) => match stack.pop() {
                        None => return Ok(completed),
                        Some(frame) => {
                            step = self.advance_import_array_frame(frame, completed)?;
                        }
                    },
                }
            }
        }
    }

    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    fn try_import_with_plugins(
        &self,
        current: &mut ArrowArrayRef,
        field: &Field,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(extension_name) = field.metadata().get(EXTENSION_TYPE_NAME_KEY) else {
            return Ok(None);
        };
        let importers = self.importers(&Id::new(extension_name));
        if importers.is_empty() {
            return Ok(None);
        }
        let dtype = self.from_arrow_field(field)?;
        let len = current.len();

        for plugin in importers.iter() {
            let input = ArrowArrayRef::clone(current);
            match plugin.from_arrow_array(input, field, &dtype, self)? {
                // Parents such as lists reuse the Arrow offsets into this array without
                // checking them again, so a plugin must preserve its length, both in what
                // it imports and in what it hands back when it declines.
                ArrowImport::Imported(arr) => {
                    vortex_ensure_eq!(
                        arr.len(),
                        len,
                        "Arrow importer {} returned {} elements for an array of {}",
                        plugin.arrow_ext_id(),
                        arr.len(),
                        len
                    );
                    return Ok(Some(arr));
                }
                ArrowImport::Unsupported(arr) => {
                    vortex_ensure_eq!(
                        arr.len(),
                        len,
                        "Arrow importer {} declined and returned {} elements for an array of {}",
                        plugin.arrow_ext_id(),
                        arr.len(),
                        len
                    );
                    *current = arr;
                }
            }
        }
        Ok(None)
    }

    fn step_from_arrow_array_canonical(
        &self,
        array: &dyn ArrowArray,
        field: &Field,
    ) -> VortexResult<ImportArrayStep> {
        match field.data_type() {
            DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
                if self.preserve_decimal_storage =>
            {
                crate::preserved_decimal::from_arrow(array, field.is_nullable())
                    .map(ImportArrayStep::Completed)
            }
            DataType::Struct(fields) => start_import_struct(array.as_struct(), fields, field),
            DataType::List(elem_field) => start_import_list::<i32>(array, elem_field, field),
            DataType::LargeList(elem_field) => start_import_list::<i64>(array, elem_field, field),
            DataType::FixedSizeList(elem_field, list_size) => {
                let fsl = array.as_fixed_size_list();
                let validity = nulls(fsl.nulls(), field.is_nullable())?;
                Ok(ImportArrayStep::Descend {
                    frame: ImportArrayFrame::FixedSizeList {
                        list_size: *list_size as u32,
                        validity,
                        len: fsl.len(),
                    },
                    array: ArrowArrayRef::clone(fsl.values()),
                    field: elem_field.as_ref().clone(),
                })
            }
            DataType::ListView(elem_field) => {
                start_import_list_view::<i32>(array, elem_field, field)
            }
            DataType::LargeListView(elem_field) => {
                start_import_list_view::<i64>(array, elem_field, field)
            }
            DataType::Map(entries_field, keys_sorted) => {
                let map = array.as_map();
                let (entries, offsets) = trim_map_entries(map);
                let entries_array: ArrowArrayRef = Arc::new(entries);
                Ok(ImportArrayStep::Descend {
                    frame: ImportArrayFrame::Map {
                        offsets,
                        map_nulls: map.nulls().cloned(),
                        keys_sorted: *keys_sorted,
                        nullable: field.is_nullable(),
                    },
                    array: entries_array,
                    field: entries_field.as_ref().clone(),
                })
            }
            DataType::RunEndEncoded(ends_field, values_field) => {
                start_import_run_end(array, ends_field, values_field, field)
            }
            _ => from_arrow_dyn(array, field.is_nullable()).map(ImportArrayStep::Completed),
        }
    }

    fn advance_import_array_frame(
        &self,
        frame: ImportArrayFrame,
        completed: ArrayRef,
    ) -> VortexResult<ImportArrayStep> {
        match frame {
            ImportArrayFrame::WrapExtension { field } => {
                match self.rebuild_ext_dtype_by_vortex_id(&field, completed.dtype())? {
                    Some(ext_dtype) => {
                        ExtensionArray::try_new(ext_dtype, completed).map(|arr| {
                            ImportArrayStep::Completed(arr.into_array())
                        })
                    }
                    None => Ok(ImportArrayStep::Completed(completed)),
                }
            }
            ImportArrayFrame::Dict { codes } => {
                // SAFETY: arrow-rs enforces the dictionary invariants on construction, so the codes
                // are in-bounds for the values.
                Ok(ImportArrayStep::Completed(
                    unsafe { DictArray::new_unchecked(codes, completed) }.into_array(),
                ))
            }
            ImportArrayFrame::Struct {
                names,
                arrow_struct,
                fields,
                next_idx,
                mut built,
                validity,
            } => {
                built.push(completed);
                let next = next_idx + 1;
                if next < fields.len() {
                    let (next_arr, next_field) =
                        prepare_struct_column(&arrow_struct, &fields, next)?;
                    Ok(ImportArrayStep::Descend {
                        frame: ImportArrayFrame::Struct {
                            names,
                            arrow_struct,
                            fields,
                            next_idx: next,
                            built,
                            validity,
                        },
                        array: next_arr,
                        field: next_field,
                    })
                } else {
                    let len = arrow_struct.len();
                    StructArray::try_new(names, built, len, validity)
                        .map(|arr| ImportArrayStep::Completed(arr.into_array()))
                }
            }
            ImportArrayFrame::List { offsets, validity } => {
                // SAFETY: Arrow offsets are non-negative and sorted, and `trim_offsets` rebases
                // them onto the referenced values. The validity has one entry per list.
                Ok(ImportArrayStep::Completed(
                    unsafe { ListArray::new_unchecked(completed, offsets, validity) }.into_array(),
                ))
            }
            ImportArrayFrame::FixedSizeList {
                list_size,
                validity,
                len,
            } => FixedSizeListArray::try_new(completed, list_size, validity, len)
                .map(|arr| ImportArrayStep::Completed(arr.into_array())),
            ImportArrayFrame::ListView {
                offsets,
                sizes,
                validity,
            } => {
                // SAFETY: Arrow list views share one integer type for offsets and sizes, and every
                // view lies within the values. The validity has one entry per list.
                Ok(ImportArrayStep::Completed(
                    unsafe { ListViewArray::new_unchecked(completed, offsets, sizes, validity) }
                        .into_array(),
                ))
            }
            ImportArrayFrame::Map {
                offsets,
                map_nulls,
                keys_sorted,
                nullable,
            } => map_from_arrow_parts(
                completed,
                &offsets,
                map_nulls.as_ref(),
                keys_sorted,
                nullable,
            )
            .map(ImportArrayStep::Completed),
            ImportArrayFrame::RunEnd { ends, offset, len } => {
                finish_run_end(ends, completed, offset, len).map(ImportArrayStep::Completed)
            }
        }
    }
}

fn variant_arrow_field(name: &str, nullable: bool) -> Field {
    // TODO(Adam): This currently encodes information about parquet-variant
    // at this level. Variant's complexity with being an essentially logical type
    // with multiple physical layout complicates handling this correctly.
    Field::new(
        name,
        DataType::Struct(
            vec![
                Field::new("metadata", DataType::BinaryView, nullable),
                Field::new("value", DataType::BinaryView, nullable),
            ]
            .into(),
        ),
        nullable,
    )
    .with_metadata(
        [(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            "arrow.parquet.variant".to_string(),
        )]
        .into(),
    )
}

enum ImportDTypeTask {
    Field(Field),
    DataType {
        data_type: DataType,
        nullability: Nullability,
    },
}

enum ImportFieldStep {
    Done(DType),
    DataType {
        wrap_field: Option<Field>,
        data_type: DataType,
        nullability: Nullability,
    },
}

enum ImportDTypeFrame {
    WrapExtension {
        field: Field,
    },
    List {
        nullability: Nullability,
    },
    FixedSizeList {
        size: u32,
        nullability: Nullability,
    },
    MapKey {
        value_field: FieldRef,
        keys_sorted: bool,
        nullability: Nullability,
    },
    MapValue {
        key_dtype: DType,
        keys_sorted: bool,
        nullability: Nullability,
    },
    Struct {
        fields: Fields,
        next_idx: usize,
        built: Vec<(FieldName, DType)>,
        nullability: Nullability,
    },
}

enum ImportDTypeStep {
    Completed(DType),
    TailField(Field),
    Descend {
        frame: ImportDTypeFrame,
        task: ImportDTypeTask,
    },
}

fn step_import_data_type(
    data_type: &DataType,
    nullability: Nullability,
) -> VortexResult<ImportDTypeStep> {
    let mut current = data_type;
    loop {
        match current {
            DataType::Dictionary(_, value_type) => {
                current = value_type.as_ref();
            }
            DataType::RunEndEncoded(_, value_field) => {
                return Ok(ImportDTypeStep::TailField(run_end_values_field(
                    value_field,
                    nullability,
                )));
            }
            DataType::List(elem)
            | DataType::LargeList(elem)
            | DataType::ListView(elem)
            | DataType::LargeListView(elem) => {
                return Ok(ImportDTypeStep::Descend {
                    frame: ImportDTypeFrame::List { nullability },
                    task: ImportDTypeTask::Field(elem.as_ref().clone()),
                });
            }
            DataType::FixedSizeList(elem, size) => {
                return Ok(ImportDTypeStep::Descend {
                    frame: ImportDTypeFrame::FixedSizeList {
                        size: *size as u32,
                        nullability,
                    },
                    task: ImportDTypeTask::Field(elem.as_ref().clone()),
                });
            }
            DataType::Map(entries, keys_sorted) => {
                vortex_ensure!(
                    !entries.is_nullable(),
                    "Arrow map entries field must be non-nullable"
                );
                let DataType::Struct(fields) = entries.data_type() else {
                    vortex_bail!(
                        "Arrow map entries field must have Struct type, got {:?}",
                        entries.data_type()
                    );
                };
                vortex_ensure_eq!(
                    fields.len(),
                    2,
                    "Arrow map entries struct must contain exactly two fields"
                );
                vortex_ensure!(
                    !fields[0].is_nullable(),
                    "Arrow map key field must be non-nullable"
                );
                return Ok(ImportDTypeStep::Descend {
                    frame: ImportDTypeFrame::MapKey {
                        value_field: FieldRef::clone(&fields[1]),
                        keys_sorted: *keys_sorted,
                        nullability,
                    },
                    task: ImportDTypeTask::Field(fields[0].as_ref().clone()),
                });
            }
            DataType::Struct(fields) => {
                let Some(first) = fields.first() else {
                    return Ok(ImportDTypeStep::Completed(DType::Struct(
                        StructFields::empty(),
                        nullability,
                    )));
                };
                return Ok(ImportDTypeStep::Descend {
                    frame: ImportDTypeFrame::Struct {
                        fields: fields.clone(),
                        next_idx: 0,
                        built: Vec::with_capacity(fields.len()),
                        nullability,
                    },
                    task: ImportDTypeTask::Field(first.as_ref().clone()),
                });
            }
            _ => {
                return from_arrow_data_type(current, nullability)
                    .map(ImportDTypeStep::Completed);
            }
        }
    }
}

enum ImportArrayFrame {
    WrapExtension {
        field: Field,
    },
    Dict {
        codes: ArrayRef,
    },
    Struct {
        names: FieldNames,
        arrow_struct: ArrowStructArray,
        fields: Fields,
        next_idx: usize,
        built: Vec<ArrayRef>,
        validity: Validity,
    },
    List {
        offsets: ArrayRef,
        validity: Validity,
    },
    FixedSizeList {
        list_size: u32,
        validity: Validity,
        len: usize,
    },
    ListView {
        offsets: ArrayRef,
        sizes: ArrayRef,
        validity: Validity,
    },
    Map {
        offsets: OffsetBuffer<i32>,
        map_nulls: Option<NullBuffer>,
        keys_sorted: bool,
        nullable: bool,
    },
    RunEnd {
        ends: ArrayRef,
        offset: usize,
        len: usize,
    },
}

#[expect(
    clippy::large_enum_variant,
    reason = "short-lived iterator step; stacked frames live in Vec"
)]
enum ImportArrayStep {
    Completed(ArrayRef),
    Descend {
        frame: ImportArrayFrame,
        array: ArrowArrayRef,
        field: Field,
    },
}

fn prepare_struct_column(
    arrow_struct: &ArrowStructArray,
    fields: &Fields,
    idx: usize,
) -> VortexResult<(ArrowArrayRef, Field)> {
    let col = arrow_struct.column(idx);
    let child_field = &fields[idx];
    // Arrow pushes nulls into non-nullable fields; strip before recursing
    // so Vortex's stricter validity invariants are upheld.
    let inner = if col.null_count() > 0 && !child_field.is_nullable() {
        make_array(remove_nulls(col.to_data())?)
    } else {
        ArrowArrayRef::clone(col)
    };
    Ok((inner, child_field.as_ref().clone()))
}

fn start_import_struct(
    arrow_struct: &ArrowStructArray,
    fields: &Fields,
    field: &Field,
) -> VortexResult<ImportArrayStep> {
    let names = FieldNames::from_iter(fields.iter().map(|fld| FieldName::from(fld.name().as_str())));
    let validity = nulls(arrow_struct.nulls(), field.is_nullable())?;
    if fields.is_empty() {
        return StructArray::try_new(names, Vec::new(), arrow_struct.len(), validity)
            .map(|arr| ImportArrayStep::Completed(arr.into_array()));
    }
    let (first_arr, first_field) = prepare_struct_column(arrow_struct, fields, 0)?;
    Ok(ImportArrayStep::Descend {
        frame: ImportArrayFrame::Struct {
            names,
            arrow_struct: arrow_struct.clone(),
            fields: fields.clone(),
            next_idx: 0,
            built: Vec::with_capacity(fields.len()),
            validity,
        },
        array: first_arr,
        field: first_field,
    })
}

fn start_import_list<O: OffsetSizeTrait + IntegerPType>(
    array: &dyn ArrowArray,
    elem_field: &FieldRef,
    field: &Field,
) -> VortexResult<ImportArrayStep> {
    let list = array.as_list::<O>();
    let (offsets, referenced) = trim_offsets(list.offsets());
    let validity = nulls(list.nulls(), field.is_nullable())?;
    Ok(ImportArrayStep::Descend {
        frame: ImportArrayFrame::List {
            offsets: offsets.into_array(),
            validity,
        },
        array: list.values().slice(referenced.start, referenced.len()),
        field: elem_field.as_ref().clone(),
    })
}

fn start_import_list_view<O: OffsetSizeTrait + IntegerPType>(
    array: &dyn ArrowArray,
    elem_field: &FieldRef,
    field: &Field,
) -> VortexResult<ImportArrayStep> {
    let list = array.as_list_view::<O>();
    let offsets = list.offsets().clone().into_array();
    let sizes = list.sizes().clone().into_array();
    let validity = nulls(list.nulls(), field.is_nullable())?;
    Ok(ImportArrayStep::Descend {
        frame: ImportArrayFrame::ListView {
            offsets,
            sizes,
            validity,
        },
        array: ArrowArrayRef::clone(list.values()),
        field: elem_field.as_ref().clone(),
    })
}

fn downcast_extract_run_end<R: RunEndIndexType>(
    array: &dyn ArrowArray,
) -> VortexResult<(ArrayRef, ArrowArrayRef, usize, usize)>
where
    R::Native: NativePType,
{
    let run_array = array
        .as_any()
        .downcast_ref::<RunArray<R>>()
        .ok_or_else(|| vortex_err!("expected an Arrow RunArray, got {}", array.data_type()))?;
    Ok(extract_run_end_parts(run_array))
}

fn start_import_run_end(
    array: &dyn ArrowArray,
    ends_field: &FieldRef,
    values_field: &FieldRef,
    field: &Field,
) -> VortexResult<ImportArrayStep> {
    let values_field = run_end_values_field(values_field, field.is_nullable().into());
    let (ends, sliced_values, offset, len) = match ends_field.data_type() {
        DataType::Int16 => downcast_extract_run_end::<Int16Type>(array)?,
        DataType::Int32 => downcast_extract_run_end::<Int32Type>(array)?,
        DataType::Int64 => downcast_extract_run_end::<Int64Type>(array)?,
        ends_dt => vortex_bail!(
            "Arrow run-end array run ends must be Int16, Int32 or Int64, got {ends_dt}"
        ),
    };
    Ok(ImportArrayStep::Descend {
        frame: ImportArrayFrame::RunEnd { ends, offset, len },
        array: sliced_values,
        field: values_field,
    })
}

/// The values field of an Arrow [`DataType::RunEndEncoded`], re-stamped with the run-end array's
/// own nullability.
fn run_end_values_field(values_field: &FieldRef, nullability: Nullability) -> Field {
    values_field
        .as_ref()
        .clone()
        .with_nullable(nullability.into())
}

/// Decode the base64 `ARROW:extension:metadata` of a field whose extension name is a Vortex ID.
///
/// A field without that key has empty metadata, as Arrow allows.
fn decode_ext_metadata(field: &Field) -> VortexResult<Vec<u8>> {
    let encoded = field.extension_type_metadata().unwrap_or_default();

    BASE64_STANDARD.decode(encoded).map_err(|err| {
        vortex_err!(
            "Arrow field {:?} must carry base64 extension metadata, got {encoded:?}: {err}",
            field.name()
        )
    })
}

impl SessionVar for ArrowSession {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// An [`ArrowSession`] bound to [`ArrowExportOptions`], created by [`ArrowSession::exporter`].
#[derive(Clone, Copy, Debug)]
pub struct ArrowExporter<'a> {
    session: &'a ArrowSession,
    options: &'a ArrowExportOptions,
}

pub(crate) enum ExecuteArrowPrep {
    Done(ArrowArrayRef),
    Naive {
        array: ArrayRef,
        data_type: Option<DataType>,
    },
}

impl ArrowExporter<'_> {
    pub(crate) fn options(&self) -> &ArrowExportOptions {
        self.options
    }

    /// Execute a Vortex array into an Arrow array with the bound options.
    ///
    /// Behaves like [`ArrowSession::execute_arrow`]. The options are propagated to nested arrays
    /// and to registered export plugins.
    pub fn execute_arrow(
        &self,
        array: ArrayRef,
        target: Option<&Field>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrowArrayRef> {
        run_export(
            self,
            ExportTask::WithField {
                array,
                target: target.cloned(),
            },
            ctx,
        )
    }

    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    pub(crate) fn prepare_execute_arrow(
        &self,
        array: ArrayRef,
        target: Option<&Field>,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ExecuteArrowPrep> {
        // NOTE(aduffy): this looks strange, but we do this to keep target_field as &Field so
        //  we can avoid cloning target when it is provided. It contains a HashMap internally that
        //  can be expensive to copy.
        let arrow_field;
        let target_field = match target {
            Some(field) => field,
            None => {
                arrow_field = self.session.default_arrow_export_field(array.dtype())?;
                &arrow_field
            }
        };

        if let Some(exported) = variant_chunks::export(self, &array, target_field, ctx)? {
            return Ok(ExecuteArrowPrep::Done(exported));
        }

        if let Some(arrow_ext_name) = target_field.metadata().get(EXTENSION_TYPE_NAME_KEY) {
            let arrow_ext_id = Id::new(arrow_ext_name);

            // There can be multiple plugins that report support for a particular extension type.
            // We try them in order until one of them reports a successful conversion.
            let len = array.len();
            let mut current = array;

            for plugin in self.session.exporters(&arrow_ext_id).iter() {
                trace!(
                    plugin = ?plugin,
                    extension_name = arrow_ext_name,
                    "probing plugin for converting Arrow array"
                );

                match plugin.execute_arrow(current, target_field, self.options, ctx)? {
                    ArrowExport::Exported(arrow) => {
                        vortex_ensure_eq!(
                            arrow.len(),
                            len,
                            "Arrow array length does not match Vortex array length after conversion to {:?}",
                            arrow
                        );
                        return Ok(ExecuteArrowPrep::Done(arrow));
                    }
                    ArrowExport::Unsupported(array) => current = array,
                }
            }

            if let Some(ext_dtype) = current
                .dtype()
                .as_extension_opt()
                .filter(|ext_dtype| ext_dtype.id() == arrow_ext_id)
                .cloned()
            {
                let storage =
                    self.unwrap_ext_storage(current, &ext_dtype, target_field, ctx)?;
                return Ok(ExecuteArrowPrep::Naive {
                    array: storage,
                    data_type: Some(target_field.data_type().clone()),
                });
            }

            debug!(
                extension_id = arrow_ext_name,
                data_type = ?target_field.data_type(),
                "unsupported Arrow extension type encountered, falling back to naive execution"
            );

            return Ok(ExecuteArrowPrep::Naive {
                array: current,
                data_type: Some(target_field.data_type().clone()),
            });
        }

        Ok(ExecuteArrowPrep::Naive {
            array,
            data_type: target.map(|field| field.data_type().clone()),
        })
    }

    /// Validate `target`'s Vortex extension metadata and unwrap `array` to its storage array.
    fn unwrap_ext_storage(
        &self,
        array: ArrayRef,
        ext_dtype: &ExtDTypeRef,
        target: &Field,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        match self
            .session
            .rebuild_ext_dtype_by_vortex_id(target, ext_dtype.storage_dtype())?
        {
            Some(target_dtype) => vortex_ensure!(
                target_dtype == *ext_dtype,
                "Arrow field {:?} must name {ext_dtype}, got {target_dtype}",
                target.name()
            ),
            None => vortex_ensure!(
                decode_ext_metadata(target)? == ext_dtype.serialize_metadata()?,
                "Arrow field {:?} must carry the metadata of {ext_dtype}, got {:?}",
                target.name(),
                target.extension_type_metadata().unwrap_or_default()
            ),
        }

        Ok(array
            .execute::<ExtensionArray>(ctx)?
            .storage_array()
            .clone())
    }
}

/// Extension trait for accessing the [`ArrowSession`] on a Vortex session.
pub trait ArrowSessionExt: SessionExt {
    /// Get the Arrow session.
    fn arrow(&self) -> SessionGuard<'_, ArrowSession>;
}

impl<S: SessionExt> ArrowSessionExt for S {
    fn arrow(&self) -> SessionGuard<'_, ArrowSession> {
        self.get::<ArrowSession>()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrow_array::DictionaryArray;
    use arrow_array::GenericListArray;
    use arrow_array::GenericListViewArray;
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_array::MapArray;
    use arrow_array::StringArray;
    use arrow_array::StructArray;
    use arrow_array::types::Int32Type;
    use arrow_buffer::OffsetBuffer;
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::FieldRef;
    use arrow_schema::Fields;
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::Dict;
    use vortex_array::arrays::ListArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::validity::Validity;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;
    use vortex_session::registry::CachedId;

    use super::*;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    #[rstest]
    #[case::prefix(0, 2)]
    #[case::middle(500, 2)]
    #[case::empty(500, 0)]
    #[case::empty_at_end(1000, 0)]
    fn test_sliced_list_imports_only_its_rows(
        #[values(false, true)] large: bool,
        #[case] start: usize,
        #[case] len: usize,
    ) -> VortexResult<()> {
        let lists = |rows: Range<i64>| -> ArrowArrayRef {
            let values = Arc::new(Int64Array::from_iter_values(
                rows.flat_map(|row| [row, row + 1]),
            ));
            let len = values.len() / 2;
            let field = Arc::new(Field::new("item", DataType::Int64, false));
            if large {
                Arc::new(GenericListArray::<i64>::new(
                    field,
                    OffsetBuffer::from_repeated_length(2, len),
                    values,
                    None,
                ))
            } else {
                Arc::new(GenericListArray::<i32>::new(
                    field,
                    OffsetBuffer::from_repeated_length(2, len),
                    values,
                    None,
                ))
            }
        };
        let session = ArrowSession::default();
        let sliced = session.from_arrow_array(lists(0..1000).slice(start, len), false)?;
        let fresh = session.from_arrow_array(lists(start as i64..(start + len) as i64), false)?;
        assert_eq!(sliced.nbytes(), fresh.nbytes());
        let mut ctx = SESSION.create_execution_ctx();
        let expected = ListArray::new(
            PrimitiveArray::from_iter(
                (start as i64..(start + len) as i64).flat_map(|row| [row, row + 1]),
            )
            .into_array(),
            PrimitiveArray::from_iter((0..=len as u64).map(|row| row * 2)).into_array(),
            Validity::NonNullable,
            &mut ctx,
        );
        assert_arrays_eq!(sliced, expected, &mut ctx);
        assert_arrays_eq!(fresh, expected, &mut ctx);
        Ok(())
    }

    #[test]
    fn from_arrow_fields_matches_schema_conversion() -> VortexResult<()> {
        let session = ArrowSession::default();
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8View, true),
        ]);
        let struct_fields = session.from_arrow_fields(&fields)?;
        let schema_dtype = session.from_arrow_schema(&Schema::new(fields))?;
        let expected = StructFields::from_iter([
            ("a", DType::Primitive(PType::I32, Nullability::NonNullable)),
            ("b", DType::Utf8(Nullability::Nullable)),
        ]);
        assert_eq!(struct_fields, expected);
        assert_eq!(
            schema_dtype,
            DType::Struct(struct_fields, Nullability::NonNullable)
        );
        Ok(())
    }

    #[test]
    fn from_arrow_field_maps_variant_without_importer() -> VortexResult<()> {
        let session = ArrowSession::default();
        let storage = DataType::Struct(
            vec![
                Field::new("metadata", DataType::BinaryView, false),
                Field::new("value", DataType::BinaryView, true),
            ]
            .into(),
        );
        let field = Field::new("v", storage, true).with_metadata(
            [(
                "ARROW:extension:name".to_string(),
                "arrow.parquet.variant".to_string(),
            )]
            .into(),
        );
        assert_eq!(
            session.from_arrow_field(&field)?,
            DType::Variant(Nullability::Nullable)
        );
        Ok(())
    }

    static REJECT_DICTIONARY_ID: CachedId = CachedId::new("test.reject_dictionary");

    /// An importer that fails on an Arrow dictionary and imports anything else as its storage.
    #[derive(Debug)]
    struct DictionaryRejectingImporter;

    impl ArrowImportVTable for DictionaryRejectingImporter {
        fn arrow_ext_id(&self) -> Id {
            *REJECT_DICTIONARY_ID
        }

        fn from_arrow_field(
            &self,
            field: &Field,
            session: &ArrowSession,
        ) -> VortexResult<Option<DType>> {
            vortex_ensure!(
                !matches!(field.data_type(), DataType::Dictionary(..)),
                "an importer must never see a dictionary field, got {}",
                field.data_type()
            );
            session
                .from_arrow_datatype(field.data_type(), field.is_nullable().into())
                .map(Some)
        }

        fn from_arrow_array(
            &self,
            array: ArrowArrayRef,
            field: &Field,
            _dtype: &DType,
            session: &ArrowSession,
        ) -> VortexResult<ArrowImport> {
            vortex_ensure!(
                !matches!(array.data_type(), DataType::Dictionary(..)),
                "an importer must never see a dictionary array, got {}",
                array.data_type()
            );
            session
                .from_arrow_array(array, field.is_nullable())
                .map(ArrowImport::Imported)
        }
    }

    /// An Arrow dictionary imports as a Vortex `Dict` array over the dictionary values, matching
    /// the dtype the schema conversion reports. The session peels the dictionary before any
    /// importer runs, so a dictionary field named for an importer that fails on dictionaries still
    /// imports.
    #[rstest]
    #[case::plain(None)]
    #[case::extension_name(Some(*REJECT_DICTIONARY_ID))]
    fn dictionary_imports_as_dict_encoding(#[case] extension_name: Option<Id>) -> VortexResult<()> {
        let session = ArrowSession::default();
        session.register_importer(Arc::new(DictionaryRejectingImporter));
        let dict: ArrowArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1, 0, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])),
        )?);
        let metadata = extension_name
            .map(|id| (EXTENSION_TYPE_NAME_KEY.to_string(), id.to_string()))
            .into_iter()
            .collect();
        let field = Field::new("s", dict.data_type().clone(), false).with_metadata(metadata);

        let dtype = session.from_arrow_field(&field)?;
        assert_eq!(dtype, DType::Utf8(Nullability::NonNullable));

        let array = session.from_arrow_array(dict, &field)?;
        assert!(array.is::<Dict>(), "expected a Dict encoding, got {array}");
        assert_eq!(array.dtype(), &dtype);
        assert_arrays_eq!(
            array,
            VarBinViewArray::from_iter_str(["a", "b", "a", "b"]),
            &mut SESSION.create_execution_ctx()
        );
        Ok(())
    }

    static TRUNCATE_ID: CachedId = CachedId::new("test.truncate");

    /// An importer that drops the last element of every array it is given, then either imports
    /// it or declines and hands it back.
    #[derive(Debug)]
    struct TruncatingImporter {
        decline: bool,
    }

    impl ArrowImportVTable for TruncatingImporter {
        fn arrow_ext_id(&self) -> Id {
            *TRUNCATE_ID
        }

        fn from_arrow_field(
            &self,
            field: &Field,
            session: &ArrowSession,
        ) -> VortexResult<Option<DType>> {
            session
                .from_arrow_datatype(field.data_type(), field.is_nullable().into())
                .map(Some)
        }

        fn from_arrow_array(
            &self,
            array: ArrowArrayRef,
            field: &Field,
            _dtype: &DType,
            session: &ArrowSession,
        ) -> VortexResult<ArrowImport> {
            let truncated = array.slice(0, array.len() - 1);
            if self.decline {
                return Ok(ArrowImport::Unsupported(truncated));
            }
            session
                .from_arrow_array(truncated, field.is_nullable())
                .map(ArrowImport::Imported)
        }
    }

    fn truncated_field(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(
            Field::new(name, data_type, false).with_metadata(
                [(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    "test.truncate".to_string(),
                )]
                .into(),
            ),
        )
    }

    fn list_with_truncated_child() -> ArrowArrayRef {
        Arc::new(GenericListArray::<i32>::new(
            truncated_field("item", DataType::Int64),
            OffsetBuffer::new(vec![0, 5].into()),
            Arc::new(Int64Array::from_iter_values(0..5)),
            None,
        ))
    }

    fn list_view_with_truncated_child() -> ArrowArrayRef {
        Arc::new(GenericListViewArray::<i32>::new(
            truncated_field("item", DataType::Int64),
            ScalarBuffer::from(vec![0]),
            ScalarBuffer::from(vec![5]),
            Arc::new(Int64Array::from_iter_values(0..5)),
            None,
        ))
    }

    fn map_with_truncated_child() -> ArrowArrayRef {
        let key_field = Arc::new(Field::new("key", DataType::Int64, false));
        let value_field = Arc::new(Field::new("value", DataType::Int64, false));
        let entries = StructArray::new(
            Fields::from(vec![key_field, value_field]),
            vec![
                Arc::new(Int64Array::from_iter_values(0..5)),
                Arc::new(Int64Array::from_iter_values(5..10)),
            ],
            None,
        );
        Arc::new(MapArray::new(
            truncated_field("entries", entries.data_type().clone()),
            OffsetBuffer::new(vec![0, 5].into()),
            entries,
            None,
            false,
        ))
    }

    #[rstest]
    #[case::list(list_with_truncated_child())]
    #[case::list_view(list_view_with_truncated_child())]
    #[case::map(map_with_truncated_child())]
    fn test_rejects_importer_that_changes_child_length(
        #[case] array: ArrowArrayRef,
        #[values(false, true)] decline: bool,
    ) {
        let session = ArrowSession::default();
        session.register_importer(Arc::new(TruncatingImporter { decline }));

        let err = session
            .from_arrow_array(array, false)
            .expect_err("a child importer that drops elements must be rejected");
        assert!(
            err.to_string()
                .contains("returned 4 elements for an array of 5"),
            "unexpected error: {err}"
        );
    }

    mod deep_tests {
        use std::sync::Arc;
        use std::thread;

        use arrow_array::Array as ArrowArray;
        use arrow_array::ArrayRef as ArrowArrayRef;
        use arrow_array::DictionaryArray;
        use arrow_array::FixedSizeBinaryArray;
        use arrow_array::FixedSizeListArray as ArrowFixedSizeListArray;
        use arrow_array::GenericListArray;
        use arrow_array::GenericListViewArray;
        use arrow_array::Int32Array;
        use arrow_array::MapArray as ArrowMapArray;
        use arrow_array::RunArray;
        use arrow_array::StringArray;
        use arrow_array::StructArray as ArrowStructArray;
        use arrow_array::types::Int32Type;
        use arrow_buffer::OffsetBuffer;
        use arrow_buffer::ScalarBuffer;
        use arrow_schema::DataType;
        use arrow_schema::Field;
        use arrow_schema::FieldRef;
        use arrow_schema::Fields;
        use vortex_array::ArrayRef;
        use vortex_array::VortexSessionExecute as _;
        use vortex_error::VortexResult;
        use vortex_error::vortex_err;
        use vortex_session::VortexSession;

        use crate::ArrowExportOptions;
        use crate::ArrowSessionExt;

        fn build_deep_session_fixture(
            depth: usize,
            unsupported_leaf: bool,
        ) -> VortexResult<(ArrowArrayRef, Field)> {
            let (mut array, mut field): (ArrowArrayRef, Field) = if unsupported_leaf {
                let leaf =
                    FixedSizeBinaryArray::try_from_iter([[1u8, 2, 3], [4u8, 5, 6]].into_iter())?;
                (
                    Arc::new(leaf),
                    Field::new("leaf", DataType::FixedSizeBinary(3), false),
                )
            } else {
                // Seed the leaf inside a DictionaryArray so dictionary import/export participates
                // without hitting arrow_cast's lack of Dictionary-over-Map packing.
                let base: ArrowArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
                let dict = DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1]),
                    base,
                )?;
                let dict_dt = dict.data_type().clone();
                (Arc::new(dict), Field::new("leaf", dict_dt, false))
            };

            for level in 0..depth {
                let kind = level % 7;
                let (next_arr, next_dt): (ArrowArrayRef, DataType) = match kind {
                    0 => {
                        let fields = Fields::from(vec![Arc::new(field)]);
                        let dt = DataType::Struct(fields.clone());
                        let st = ArrowStructArray::new(fields, vec![array], None);
                        (Arc::new(st), dt)
                    }
                    1 => {
                        let child_field = Arc::new(field);
                        let dt = DataType::List(Arc::clone(&child_field));
                        let list = GenericListArray::<i32>::new(
                            child_field,
                            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                            array,
                            None,
                        );
                        (Arc::new(list), dt)
                    }
                    2 => {
                        let child_field = Arc::new(field);
                        let dt = DataType::LargeList(Arc::clone(&child_field));
                        let list = GenericListArray::<i64>::new(
                            child_field,
                            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 1, 2])),
                            array,
                            None,
                        );
                        (Arc::new(list), dt)
                    }
                    3 => {
                        let child_field = Arc::new(field);
                        let dt = DataType::FixedSizeList(Arc::clone(&child_field), 1);
                        let fsl = ArrowFixedSizeListArray::new(child_field, 1, array, None);
                        (Arc::new(fsl), dt)
                    }
                    4 => {
                        let child_field = Arc::new(field);
                        let dt = DataType::ListView(Arc::clone(&child_field));
                        let lv = GenericListViewArray::<i32>::new(
                            child_field,
                            ScalarBuffer::from(vec![0i32, 1]),
                            ScalarBuffer::from(vec![1i32, 1]),
                            array,
                            None,
                        );
                        (Arc::new(lv), dt)
                    }
                    5 => {
                        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
                        let val_field = Arc::new(field);
                        let entry_fields = Fields::from(vec![key_field, val_field]);
                        let entries = ArrowStructArray::new(
                            entry_fields.clone(),
                            vec![Arc::new(Int32Array::from(vec![1, 2])), array],
                            None,
                        );
                        let entries_field = Arc::new(Field::new_struct(
                            "entries",
                            entry_fields,
                            false,
                        ));
                        let dt = DataType::Map(Arc::clone(&entries_field), false);
                        let map = ArrowMapArray::new(
                            entries_field,
                            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                            entries,
                            None,
                            false,
                        );
                        (Arc::new(map), dt)
                    }
                    _ => {
                        let run_ends = Int32Array::from(vec![1, 2]);
                        let runs = RunArray::<Int32Type>::try_new(&run_ends, array.as_ref())?;
                        let dt = runs.data_type().clone();
                        (Arc::new(runs), dt)
                    }
                };
                field = Field::new(format!("c_{level}"), next_dt, false);
                array = next_arr;
            }
            Ok((array, field))
        }

        #[test]
        fn deep_dictionary_nesting_imports_iteratively_on_small_stack() -> VortexResult<()> {
            let depth = 2048;
            let mut array: ArrowArrayRef =
                Arc::new(StringArray::from(vec!["alpha", "beta"]));
            for _ in 0..depth {
                array = Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1]),
                    array,
                )?);
            }
            let field = Field::new("dict_chain", array.data_type().clone(), false);

            let handle = thread::Builder::new()
                .stack_size(128 * 1024)
                .spawn(move || -> VortexResult<(ArrayRef, ArrowArrayRef)> {
                    let vx_session = VortexSession::empty();
                    let arrow_session = vx_session.arrow();
                    let vx_dtype = arrow_session.from_arrow_field(&field)?;
                    let vx_arr = arrow_session.from_arrow_array(array, &field)?;
                    assert_eq!(vx_arr.dtype(), &vx_dtype);
                    let mut ctx = vx_session.create_execution_ctx();
                    let options = ArrowExportOptions::default();
                    let arrow_out = arrow_session
                        .exporter(&options)
                        .execute_arrow(vx_arr.clone(), Some(&field), &mut ctx)?;
                    Ok((vx_arr, arrow_out))
                })
                .map_err(|err| vortex_err!("spawn 128 KiB thread: {err}"))?;

            let (vx_arr, arrow_out) = handle
                .join()
                .map_err(|err| vortex_err!("128 KiB thread panicked: {err:?}"))??;
            assert_eq!(vx_arr.len(), 2);
            assert_eq!(arrow_out.len(), 2);
            Ok(())
        }

        #[test]
        fn deep_session_schema_and_array_roundtrip_on_small_stack() -> VortexResult<()> {
            let depth = 512;
            let (arrow_in, field_in) = build_deep_session_fixture(depth, false)?;
            let (arrow_err, field_err) = build_deep_session_fixture(depth, true)?;
            // Inferred `None`-target export converts RunEndArray through canonical execution, which
            // supports primitive/string/binary values rather than Map/Struct run values, so build a
            // separate 512-deep no-REE fixture to exercise `None`-target export on 128 KiB stack.
            let mut inferred_arr: ArrowArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
            let mut inferred_field = Field::new("leaf", DataType::Int32, false);
            for level in 0..depth {
                let child_field = Arc::new(inferred_field);
                let dt = DataType::List(Arc::clone(&child_field));
                let list = GenericListArray::<i32>::new(
                    child_field,
                    OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                    inferred_arr,
                    None,
                );
                inferred_arr = Arc::new(list);
                inferred_field = Field::new(format!("i_{level}"), dt, false);
            }

            let handle = thread::Builder::new()
                .stack_size(128 * 1024)
                .spawn(move || -> VortexResult<(ArrayRef, ArrowArrayRef, ArrowArrayRef)> {
                    let vx_session = VortexSession::empty();
                    let arrow_session = vx_session.arrow();

                    let vx_dtype = arrow_session.from_arrow_field(&field_in)?;
                    let rt_field = arrow_session.to_arrow_field(field_in.name(), &vx_dtype)?;
                    assert_eq!(rt_field.name(), field_in.name());
                    let fields = Fields::from(vec![FieldRef::new(field_in.clone())]);
                    let vx_fields = arrow_session.from_arrow_fields(&fields)?;
                    assert_eq!(vx_fields.field_by_index(0), Some(vx_dtype));

                    let vx_arr = arrow_session.from_arrow_array(arrow_in, &field_in)?;
                    let vx_inferred_input =
                        arrow_session.from_arrow_array(inferred_arr, &inferred_field)?;
                    let mut ctx = vx_session.create_execution_ctx();
                    let options = ArrowExportOptions::default();
                    let exporter = arrow_session.exporter(&options);
                    let arrow_out =
                        exporter.execute_arrow(vx_arr.clone(), Some(&field_in), &mut ctx)?;
                    let arrow_inferred =
                        exporter.execute_arrow(vx_inferred_input, None, &mut ctx)?;
                    assert_eq!(arrow_out.len(), 2);
                    assert_eq!(arrow_inferred.len(), 2);

                    assert!(arrow_session.from_arrow_array(arrow_err, &field_err).is_err());
                    Ok((vx_arr, arrow_out, arrow_inferred))
                })
                .map_err(|err| vortex_err!("spawn 128 KiB thread: {err}"))?;

            let (vx_arr, arrow_out, arrow_inferred) = handle
                .join()
                .map_err(|err| vortex_err!("128 KiB thread panicked: {err:?}"))??;
            assert_eq!(vx_arr.len(), 2);
            assert_eq!(arrow_out.len(), 2);
            assert_eq!(arrow_inferred.len(), 2);
            Ok(())
        }

        #[test]
        fn deep_2048_session_roundtrip_without_overflow() -> VortexResult<()> {
            let depth = 2048;
            let (arrow_in, field_in) = build_deep_session_fixture(depth, false)?;
            let vx_session = VortexSession::empty();
            let arrow_session = vx_session.arrow();

            let vx_dtype = arrow_session.from_arrow_field(&field_in)?;
            let rt_field = arrow_session.to_arrow_field(field_in.name(), &vx_dtype)?;
            assert_eq!(rt_field.name(), field_in.name());

            let vx_arr = arrow_session.from_arrow_array(ArrowArrayRef::clone(&arrow_in), &field_in)?;
            let mut ctx = vx_session.create_execution_ctx();
            let options = ArrowExportOptions::default();
            let arrow_out = arrow_session
                .exporter(&options)
                .execute_arrow(vx_arr, Some(&field_in), &mut ctx)?;
            assert_eq!(arrow_out.len(), arrow_in.len());
            assert_eq!(arrow_out.data_type(), arrow_in.data_type());
            Ok(())
        }

        #[test]
        fn shallow_parity_session_roundtrip_preserves_data() -> VortexResult<()> {
            let (arrow_in, field_in) = build_deep_session_fixture(6, false)?;
            let vx_session = VortexSession::empty();
            let arrow_session = vx_session.arrow();
            let vx_arr = arrow_session.from_arrow_array(ArrowArrayRef::clone(&arrow_in), &field_in)?;
            let mut ctx = vx_session.create_execution_ctx();
            let options = ArrowExportOptions::default();
            let arrow_out = arrow_session
                .exporter(&options)
                .execute_arrow(vx_arr, Some(&field_in), &mut ctx)?;
            assert_eq!(arrow_out.to_data(), arrow_in.to_data());
            Ok(())
        }
    }
}
