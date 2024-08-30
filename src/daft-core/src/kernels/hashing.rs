use arrow2::{
    array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, NullArray, PrimitiveArray,
        Utf8Array,
    },
    bitmap::MutableBitmap,
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};

use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

mod const_hashed {
    use xxhash_rust::const_xxh3;

    pub(super) const NULL_HASH: u64 = const_xxh3::xxh3_64(b"");
    pub(super) const TRUE_HASH: u64 = const_xxh3::xxh3_64(b"1");
    pub(super) const FALSE_HASH: u64 = const_xxh3::xxh3_64(b"0");
}

fn base_hasher<T, E: ExactSizeIterator<Item = T>>(
    array: impl IntoIterator<IntoIter = E>,
    seed: Option<&PrimitiveArray<u64>>,
    check_validity: fn(&T) -> bool,
    with_seed: impl Fn(T, u64) -> u64,
    without_seed: impl Fn(T) -> u64,
) -> PrimitiveArray<u64> {
    let array = array.into_iter();
    let array_len = array.len();
    let (hashes, bitmap) = match seed {
        Some(seed) => {
            let seed_len = seed.len();
            assert_eq!(array_len, seed_len, "Oops! Array and seed must have the same length; instead array length was {} and seed length was {}", array_len, seed_len);
            array.zip(seed.values_iter().copied()).fold(
                (
                    Vec::with_capacity(array_len),
                    MutableBitmap::with_capacity(array_len),
                ),
                |(mut hashes, mut bitmap), (value, seed_value)| {
                    let validity = check_validity(&value);
                    let hash = with_seed(value, seed_value);
                    hashes.push(hash);
                    bitmap.push(validity);
                    (hashes, bitmap)
                },
            )
        }
        None => array.fold(
            (
                Vec::with_capacity(array_len),
                MutableBitmap::with_capacity(array_len),
            ),
            |(mut hashes, mut bitmap), value| {
                let validity = check_validity(&value);
                let hash = without_seed(value);
                hashes.push(hash);
                bitmap.push(validity);
                (hashes, bitmap)
            },
        ),
    };
    let bitmap = bitmap.into();
    PrimitiveArray::new(DataType::UInt64, hashes.into(), Some(bitmap))
}

fn hash_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    base_hasher(
        array,
        seed,
        Option::is_some,
        |v, s| match v {
            Some(v) => xxh3_64_with_seed(v.to_le_bytes().as_ref(), s),
            None => const_hashed::NULL_HASH,
        },
        |v| match v {
            Some(v) => xxh3_64(v.to_le_bytes().as_ref()),
            None => const_hashed::NULL_HASH,
        },
    )
}

fn hash_boolean(array: &BooleanArray, seed: Option<&PrimitiveArray<u64>>) -> PrimitiveArray<u64> {
    base_hasher(
        array,
        seed,
        Option::is_some,
        |v, s| match v {
            Some(true) => xxh3_64_with_seed(b"1", s),
            Some(false) => xxh3_64_with_seed(b"0", s),
            None => const_hashed::NULL_HASH,
        },
        |v| match v {
            Some(true) => const_hashed::TRUE_HASH,
            Some(false) => const_hashed::FALSE_HASH,
            None => const_hashed::NULL_HASH,
        },
    )
}

fn hash_null(array: &NullArray, seed: Option<&PrimitiveArray<u64>>) -> PrimitiveArray<u64> {
    let hashes = if let Some(seed) = seed {
        seed.values_iter()
            .map(|&s| xxh3_64_with_seed(b"", s))
            .collect::<Vec<_>>()
    } else {
        (0..array.len())
            .map(|_| const_hashed::NULL_HASH)
            .collect::<Vec<_>>()
    };
    PrimitiveArray::<u64>::new(DataType::UInt64, hashes.into(), None)
}

fn hash_binary<O: Offset>(
    array: &BinaryArray<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    base_hasher(
        array.values_iter(),
        seed,
        |_| true,
        xxh3_64_with_seed,
        xxh3_64,
    )
}

fn hash_fixed_size_binary(
    array: &FixedSizeBinaryArray,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    base_hasher(
        array.values_iter(),
        seed,
        |_| true,
        xxh3_64_with_seed,
        xxh3_64,
    )
}

fn hash_utf8<O: Offset>(
    array: &Utf8Array<O>,
    seed: Option<&PrimitiveArray<u64>>,
) -> PrimitiveArray<u64> {
    base_hasher(
        array.values_iter(),
        seed,
        |_| true,
        |v, s| xxh3_64_with_seed(v.as_bytes(), s),
        |v| xxh3_64(v.as_bytes()),
    )
}

macro_rules! with_match_hashing_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use arrow2::datatypes::PrimitiveType::*;
    use arrow2::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        DaysMs => __with_ty__! { days_ms },
        MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => return Err(Error::NotYetImplemented(format!(
            "Hash not implemented for type {:?}",
            $key_type
        )))
    }
})}

pub fn hash(array: &dyn Array, seed: Option<&PrimitiveArray<u64>>) -> Result<PrimitiveArray<u64>> {
    if let Some(s) = seed {
        if s.len() != array.len() {
            return Err(Error::InvalidArgumentError(format!(
                "seed length does not match array length: {} vs {}",
                s.len(),
                array.len()
            )));
        }

        if *s.data_type() != DataType::UInt64 {
            return Err(Error::InvalidArgumentError(format!(
                "seed data type expected to be uint64, got {:?}",
                *s.data_type()
            )));
        }
    }

    use PhysicalType::*;
    Ok(match array.data_type().to_physical_type() {
        Null => hash_null(array.as_any().downcast_ref().unwrap(), seed),
        Boolean => hash_boolean(array.as_any().downcast_ref().unwrap(), seed),
        Primitive(primitive) => with_match_hashing_primitive_type!(primitive, |$T| {
            hash_primitive::<$T>(array.as_any().downcast_ref().unwrap(), seed)
        }),
        Binary => hash_binary::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        LargeBinary => hash_binary::<i64>(array.as_any().downcast_ref().unwrap(), seed),
        FixedSizeBinary => hash_fixed_size_binary(array.as_any().downcast_ref().unwrap(), seed),
        Utf8 => hash_utf8::<i32>(array.as_any().downcast_ref().unwrap(), seed),
        LargeUtf8 => hash_utf8::<i64>(array.as_any().downcast_ref().unwrap(), seed),
        t => {
            return Err(Error::NotYetImplemented(format!(
                "Hash not implemented for type {t:?}"
            )))
        }
    })
}
