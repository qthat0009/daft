use crate::datatypes::DataType;
use crate::error::DaftError;
use crate::error::DaftResult;

/// Largely influenced by polars supertype logic which is based on numpy / python type propagation

pub fn try_get_supertype(l: &DataType, r: &DataType) -> DaftResult<DataType> {
    match get_supertype(l, r) {
        Some(dt) => Ok(dt),
        None => Err(DaftError::TypeError(format!(
            "could not determine supertype of {:?} and {:?}",
            l, r
        ))),
    }
}

// pub fn get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
//     use DataType::*;
//     println!("get_supertype: {:?}, {:?}", l, r);

//     match (l, r) {
//         (Arrow(l), Arrow(r)) => get_arrow_supertype(l, r).map(DataType::Arrow),
//         _ => None,
//     }
// }

pub fn get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    fn inner(l: &DataType, r: &DataType) -> Option<DataType> {
        use DataType::*;

        if l == r {
            return Some(l.clone());
        }

        match (l, r) {
            (Int8, Boolean) => Some(Int8),
            (Int8, Int16) => Some(Int16),
            (Int8, Int32) => Some(Int32),
            (Int8, Int64) => Some(Int64),
            (Int8, UInt8) => Some(Int16),
            (Int8, UInt16) => Some(Int32),
            (Int8, UInt32) => Some(Int64),
            (Int8, UInt64) => Some(Float64), // Follow numpy
            (Int8, Float32) => Some(Float32),
            (Int8, Float64) => Some(Float64),

            (Int16, Boolean) => Some(Int16),
            (Int16, Int8) => Some(Int16),
            (Int16, Int32) => Some(Int32),
            (Int16, Int64) => Some(Int64),
            (Int16, UInt8) => Some(Int16),
            (Int16, UInt16) => Some(Int32),
            (Int16, UInt32) => Some(Int64),
            (Int16, UInt64) => Some(Float64), // Follow numpy
            (Int16, Float32) => Some(Float32),
            (Int16, Float64) => Some(Float64),

            (Int32, Boolean) => Some(Int32),
            (Int32, Int8) => Some(Int32),
            (Int32, Int16) => Some(Int32),
            (Int32, Int64) => Some(Int64),
            (Int32, UInt8) => Some(Int32),
            (Int32, UInt16) => Some(Int32),
            (Int32, UInt32) => Some(Int64),
            (Int32, UInt64) => Some(Float64),  // Follow numpy
            (Int32, Float32) => Some(Float64), // Follow numpy
            (Int32, Float64) => Some(Float64),

            (Int64, Boolean) => Some(Int64),
            (Int64, Int8) => Some(Int64),
            (Int64, Int16) => Some(Int64),
            (Int64, Int32) => Some(Int64),
            (Int64, UInt8) => Some(Int64),
            (Int64, UInt16) => Some(Int64),
            (Int64, UInt32) => Some(Int64),
            (Int64, UInt64) => Some(Float64),  // Follow numpy
            (Int64, Float32) => Some(Float64), // Follow numpy
            (Int64, Float64) => Some(Float64),

            (UInt16, UInt8) => Some(UInt16),
            (UInt16, UInt32) => Some(UInt32),
            (UInt16, UInt64) => Some(UInt64),

            (UInt8, UInt32) => Some(UInt32),
            (UInt8, UInt64) => Some(UInt64),
            (UInt32, UInt64) => Some(UInt64),

            (Boolean, UInt8) => Some(UInt8),
            (Boolean, UInt16) => Some(UInt16),
            (Boolean, UInt32) => Some(UInt32),
            (Boolean, UInt64) => Some(UInt64),

            (Float32, UInt8) => Some(Float32),
            (Float32, UInt16) => Some(Float32),
            (Float32, UInt32) => Some(Float64),
            (Float32, UInt64) => Some(Float64),

            (Float64, UInt8) => Some(Float64),
            (Float64, UInt16) => Some(Float64),
            (Float64, UInt32) => Some(Float64),
            (Float64, UInt64) => Some(Float64),

            (Float64, Float32) => Some(Float64),
            //TODO(sammy): add time, struct related dtypes
            (Boolean, Float32) => Some(Float32),
            (Boolean, Float64) => Some(Float64),

            // every known type can be casted to a string except binary
            (dt, Utf8) if dt.ne(&Binary) => Some(Utf8),
            (dt, Null) => Some(dt.clone()), // Drop Null Type

            _ => None,
        }
    }
    match inner(l, r) {
        Some(dt) => Some(dt),
        None => inner(r, l),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_bad_arrow_type() -> DaftResult<()> {
        let result = get_supertype(&DataType::Utf8, &DataType::Binary);
        assert_eq!(result, None);
        Ok(())
    }
}
