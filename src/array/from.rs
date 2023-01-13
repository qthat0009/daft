use core::slice;
use std::sync::Arc;

use crate::datatypes::{DaftDataType, DaftNumericType, DataType, Field, Utf8Array, Utf8Type};

use crate::array::DataArray;

impl<T: DaftNumericType> From<Box<arrow2::array::PrimitiveArray<T::Native>>> for DataArray<T> {
    fn from(item: Box<arrow2::array::PrimitiveArray<T::Native>>) -> Self {
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            item.arced(),
        )
        .unwrap()
    }
}

impl<T> From<&[T::Native]> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(slice: &[T::Native]) -> Self {
        let arrow_array = arrow2::array::PrimitiveArray::<T::Native>::from_slice(slice);
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

impl<T: AsRef<str>> From<&[T]> for DataArray<Utf8Type> {
    fn from(slice: &[T]) -> Self {
        let arrow_array = arrow2::array::Utf8Array::<i64>::from_slice(slice);
        DataArray::new(
            Field::new("arrow_array", DataType::Utf8).into(),
            arrow_array.arced(),
        )
        .unwrap()
    }
}

// impl Utf8Array
// {
//     fn from<T: AsRef<str>>(slice: &[T]) -> Self {
//         let arrow_array = arrow2::array::Utf8Array::<i64>::from_slice(slice);
//         DataArray::new(
//             Field::new("arrow_array", DataType::Utf8).into(),
//             arrow_array.arced(),
//         )
//         .unwrap()
//     }
// }

impl<T: DaftDataType> From<Box<dyn arrow2::array::Array>> for DataArray<T> {
    fn from(item: Box<dyn arrow2::array::Array>) -> Self {
        let self_arrow_type = T::get_dtype().to_arrow().unwrap();
        if !item.data_type().eq(&self_arrow_type) {
            panic!(
                "mismatch in expected data type {:?} vs {:?}",
                item.data_type(),
                self_arrow_type
            )
        }
        DataArray::new(
            Field::new("arrow_array", T::get_dtype()).into(),
            Arc::from(item),
        )
        .unwrap()
    }
}

// pub fn from_arrow(array: Box<dyn arrow2::array::Array>) -> Box<dyn BaseArray> {
//     let daft_data_type = DataType::from(array.data_type());
//     with_match_physical_numeric_daft_type!(daft_data_type, |$T| {DataArray::<$T>::from(array).boxed()})
// }
