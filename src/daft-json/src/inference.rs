use std::{borrow::Borrow, collections::HashSet};

use arrow2::datatypes::{DataType, Field, Metadata, Schema};
use arrow2::error::{Error, Result};
use indexmap::IndexMap;
use json_deserializer::{Number, Value};

const ITEM_NAME: &str = "item";

/// Infer Arrow2 schema from JSON Value record.
pub(crate) fn infer_records_schema(record: &Value) -> Result<Schema> {
    let fields = match record {
        Value::Object(record) => record
            .iter()
            .map(|(name, value)| {
                let data_type = infer(value)?;

                Ok(Field {
                    name: name.clone(),
                    data_type,
                    is_nullable: true,
                    metadata: Metadata::default(),
                })
            })
            .collect::<Result<Vec<_>>>(),
        _ => Err(Error::ExternalFormat(
            "Deserialized JSON value is not an Object record".to_string(),
        )),
    }?;

    Ok(Schema {
        fields,
        metadata: Metadata::default(),
    })
}

/// Infers [`DataType`] from [`Value`].
fn infer(json: &Value) -> Result<DataType> {
    Ok(match json {
        Value::Bool(_) => DataType::Boolean,
        Value::Array(array) => infer_array(array)?,
        Value::Null => DataType::Null,
        Value::Number(number) => infer_number(number),
        Value::String(string) => infer_string(string),
        Value::Object(inner) => infer_object(inner)?,
    })
}

fn infer_string(string: &str) -> DataType {
    daft_decoding::inference::infer_string(string)
}

fn infer_object(inner: &IndexMap<String, Value>) -> Result<DataType> {
    let fields = inner
        .iter()
        .map(|(key, value)| infer(value).map(|dt| (key, dt)))
        .map(|maybe_dt| {
            let (key, dt) = maybe_dt?;
            Ok(Field::new(key, dt, true))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DataType::Struct(fields))
}

fn infer_array(values: &[Value]) -> Result<DataType> {
    let types = values
        .iter()
        .map(infer)
        // Deduplicate dtypes.
        .collect::<Result<HashSet<_>>>()?;

    let dt = if !types.is_empty() {
        let types = types.into_iter().collect::<Vec<_>>();
        coerce_data_type(&types)
    } else {
        DataType::Null
    };

    // Don't add a record that contains only nulls.
    Ok(if dt == DataType::Null {
        dt
    } else {
        DataType::List(Box::new(Field::new(ITEM_NAME, dt, true)))
    })
}

fn infer_number(n: &Number) -> DataType {
    match n {
        Number::Float(..) => DataType::Float64,
        Number::Integer(..) => DataType::Int64,
    }
}

/// Convert each column's set of infered dtypes to a field with a consolidated dtype, following the coercion rules
/// defined in coerce_data_type.
pub(crate) fn column_types_map_to_fields(
    column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>>,
) -> Vec<arrow2::datatypes::Field> {
    column_types
        .into_iter()
        .map(|(name, dtype_set)| {
            let dtypes = dtype_set.into_iter().collect::<Vec<_>>();
            // Get consolidated dtype for column.
            let dtype = coerce_data_type(dtypes.as_slice());
            arrow2::datatypes::Field::new(name, dtype, true)
        })
        .collect::<Vec<_>>()
}

/// Coerce an heterogeneous set of [`DataType`] into a single one. Rules:
/// * The empty set is coerced to `Null`
/// * `Int64` and `Float64` are `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * Structs contain the union of all fields
/// * All other types are coerced to `Utf8`
pub(crate) fn coerce_data_type<A: Borrow<DataType> + std::fmt::Debug>(datatypes: &[A]) -> DataType {
    // Drop null dtype from the dtype set.
    let datatypes = datatypes
        .iter()
        .filter(|dt| !matches!((*dt).borrow(), DataType::Null))
        .collect::<Vec<_>>();

    if datatypes.is_empty() {
        return DataType::Null;
    }

    let are_all_equal = datatypes.windows(2).all(|w| w[0].borrow() == w[1].borrow());

    if are_all_equal {
        return datatypes[0].borrow().clone();
    }

    let are_all_structs = datatypes
        .iter()
        .all(|x| matches!((*x).borrow(), DataType::Struct(_)));

    if are_all_structs {
        // All structs => union of all field dtypes (these may have equal names).
        let fields = datatypes.iter().fold(vec![], |mut acc, dt| {
            if let DataType::Struct(new_fields) = (*dt).borrow() {
                acc.extend(new_fields);
            };
            acc
        });
        // Group fields by unique names.
        let fields = fields.iter().fold(
            IndexMap::<&String, HashSet<&DataType>>::new(),
            |mut acc, field| {
                match acc.entry(&field.name) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().insert(&field.data_type);
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        let mut a = HashSet::new();
                        a.insert(&field.data_type);
                        v.insert(a);
                    }
                }
                acc
            },
        );
        // Coerce dtype set for each field.
        let fields = fields
            .into_iter()
            .map(|(name, dts)| {
                let dts = dts.into_iter().collect::<Vec<_>>();
                Field::new(name, coerce_data_type(&dts), true)
            })
            .collect();
        return DataType::Struct(fields);
    } else if datatypes.len() > 2 {
        // TODO(Clark): Return an error for uncoercible types.
        return DataType::Utf8;
    }
    let (lhs, rhs) = (datatypes[0].borrow(), datatypes[1].borrow());

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (DataType::List(lhs), DataType::List(rhs)) => {
            let inner = coerce_data_type(&[lhs.data_type(), rhs.data_type()]);
            DataType::List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (scalar, DataType::List(list)) | (DataType::List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            DataType::List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (DataType::Float64, DataType::Int64) | (DataType::Int64, DataType::Float64) => {
            DataType::Float64
        }
        (DataType::Int64, DataType::Boolean) | (DataType::Boolean, DataType::Int64) => {
            DataType::Int64
        }
        (_, _) => DataType::Utf8,
    };
}
