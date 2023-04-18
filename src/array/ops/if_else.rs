use crate::array::{BaseArray, DataArray};
use crate::datatypes::{
    BinaryArray, BooleanArray, DaftNumericType, FixedSizeListArray, ListArray, NullArray,
    PythonArray, StructArray, Utf8Array,
};
use crate::error::{DaftError, DaftResult};
use arrow2::compute::if_then_else::if_then_else;
use std::convert::identity;

use super::broadcast::Broadcastable;
use super::downcast::Downcastable;

// Helper macro for broadcasting if/else across the if_true/if_false/predicate DataArrays
//
// `array_type`: the Arrow2 array type that if_true/if_false should be downcasted to
// `if_true/if_false`: the DataArrays to select data from, conditional on the predicate
// `predicate`: the predicate boolean DataArray
// `scalar_copy`: a simple inlined function to run on each borrowed scalar value when iterating through if_true/if_else.
//   Note that this is potentially the identity function if Arrow2 allows for creation of this Array from borrowed data (e.g. &str)
macro_rules! broadcast_if_else{(
    $array_type:ty, $if_true:expr, $if_false:expr, $predicate:expr, $scalar_copy:expr,
) => ({
    match ($if_true.len(), $if_false.len(), $predicate.len()) {
        // CASE: Equal lengths across all 3 arguments
        (self_len, other_len, predicate_len) if self_len == other_len && other_len == predicate_len => {
            let result = if_then_else($predicate.downcast(), $if_true.data(), $if_false.data())?;
            DataArray::try_from(($if_true.name(), result))
        },
        // CASE: Broadcast predicate
        (self_len, _, 1) => {
            let predicate_scalar = $predicate.get(0);
            match predicate_scalar {
                None => Ok(DataArray::full_null($if_true.name(), self_len)),
                Some(predicate_scalar_value) => {
                    if predicate_scalar_value {
                        Ok($if_true.clone())
                    } else {
                        Ok($if_false.clone())
                    }
                }
            }
        }
        // CASE: Broadcast both arrays
        (1, 1, _) => {
            let self_scalar = $if_true.get(0);
            let other_scalar = $if_false.get(0);
            let predicate_arr = $predicate.downcast();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = predicate_values.iter().map(
                |pred_val| match pred_val {
                    true => self_scalar,
                    false => other_scalar,
                }
            ).collect();
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
        }
        // CASE: Broadcast truthy array
        (1, o, p)  if o == p => {
            let self_scalar = $if_true.get(0);
            let predicate_arr = $predicate.downcast();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = $if_false.downcast().iter().zip(predicate_values.iter()).map(
                |(other_val, pred_val)| match pred_val {
                    true => self_scalar,
                    false => $scalar_copy(other_val),
                }
            ).collect();
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
        }
        // CASE: Broadcast falsey array
        (s, 1, p)  if s == p => {
            let other_scalar = $if_false.get(0);
            let predicate_arr = $predicate.downcast();
            let predicate_values = predicate_arr.values();
            let naive_if_else: $array_type = $if_true.downcast().iter().zip(predicate_values.iter()).map(
                |(self_val, pred_val)| match pred_val {
                    true => $scalar_copy(self_val),
                    false => other_scalar,
                }
            ).collect();
            DataArray::new($if_true.field.clone(), Box::new(naive_if_else.with_validity(predicate_arr.validity().cloned())))
        }
        (s, o, p) => Err(DaftError::ValueError(format!("Cannot run if_else against arrays with non-broadcastable lengths: self={s}, other={o}, predicate={p}")))
    }
})}

#[inline(always)]
fn copy_optional_native<T: DaftNumericType>(v: Option<&T::Native>) -> Option<T::Native> {
    v.copied()
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn if_else(
        &self,
        other: &DataArray<T>,
        predicate: &BooleanArray,
    ) -> DaftResult<DataArray<T>> {
        broadcast_if_else!(
            arrow2::array::PrimitiveArray::<T::Native>,
            self,
            other,
            predicate,
            copy_optional_native::<T>,
        )
    }
}

impl Utf8Array {
    pub fn if_else(&self, other: &Utf8Array, predicate: &BooleanArray) -> DaftResult<Utf8Array> {
        broadcast_if_else!(
            arrow2::array::Utf8Array<i64>,
            self,
            other,
            predicate,
            identity,
        )
    }
}

impl BooleanArray {
    pub fn if_else(
        &self,
        other: &BooleanArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BooleanArray> {
        broadcast_if_else!(
            arrow2::array::BooleanArray,
            self,
            other,
            predicate,
            identity,
        )
    }
}

impl BinaryArray {
    pub fn if_else(
        &self,
        other: &BinaryArray,
        predicate: &BooleanArray,
    ) -> DaftResult<BinaryArray> {
        broadcast_if_else!(
            arrow2::array::BinaryArray<i64>,
            self,
            other,
            predicate,
            identity,
        )
    }
}

impl NullArray {
    pub fn if_else(&self, _other: &NullArray, _predicate: &BooleanArray) -> DaftResult<NullArray> {
        Ok(DataArray::full_null(self.name(), self.len()))
    }
}

impl PythonArray {
    pub fn if_else(
        &self,
        _other: &PythonArray,
        _predicate: &BooleanArray,
    ) -> DaftResult<PythonArray> {
        todo!("[RUST-INT][PY]");
    }
}

fn from_arrow_if_then_else<T>(predicate: &BooleanArray, if_true: &T, if_false: &T) -> DaftResult<T>
where
    T: BaseArray
        + Downcastable
        + for<'a> TryFrom<(&'a str, Box<dyn arrow2::array::Array>), Error = DaftError>,
    <T as Downcastable>::Output: arrow2::array::Array,
{
    let result = if_then_else(
        predicate.downcast(),
        if_true.downcast(),
        if_false.downcast(),
    )?;
    T::try_from((if_true.name(), result))
}

fn nested_if_then_else<T>(predicate: &BooleanArray, if_true: &T, if_false: &T) -> DaftResult<T>
where
    T: BaseArray
        + Broadcastable
        + Downcastable
        + for<'a> TryFrom<(&'a str, Box<dyn arrow2::array::Array>), Error = DaftError>,
    <T as Downcastable>::Output: arrow2::array::Array,
{
    // TODO(Clark): Support streaming broadcasting, i.e. broadcasting without inflating scalars to full array length.
    match (predicate.len(), if_true.len(), if_false.len()) {
        (predicate_len, if_true_len, if_false_len)
            if predicate_len == if_true_len && if_true_len == if_false_len => from_arrow_if_then_else(predicate, if_true, if_false),
        (1, if_true_len, 1) => from_arrow_if_then_else(
            &predicate.broadcast(if_true_len)?,
            if_true,
            &if_false.broadcast(if_true_len)?,
        ),
        (1, 1, if_false_len) => from_arrow_if_then_else(
            &predicate.broadcast(if_false_len)?,
            &if_true.broadcast(if_false_len)?,
            if_false,
        ),
        (predicate_len, 1, 1) => from_arrow_if_then_else(
            predicate,
            &if_true.broadcast(predicate_len)?,
            &if_false.broadcast(predicate_len)?,
        ),
        (predicate_len, if_true_len, 1) if predicate_len == if_true_len => from_arrow_if_then_else(
            predicate,
            if_true,
            &if_false.broadcast(predicate_len)?,
        ),
        (predicate_len, 1, if_false_len) if predicate_len == if_false_len => from_arrow_if_then_else(
            predicate,
            &if_true.broadcast(predicate_len)?,
            if_false,
        ),
        (p, s, o) => {
            Err(DaftError::ValueError(format!("Cannot run if_else against arrays with non-broadcastable lengths: if_true={s}, if_false={o}, predicate={p}")))
        }
    }
}

impl ListArray {
    pub fn if_else(&self, other: &ListArray, predicate: &BooleanArray) -> DaftResult<ListArray> {
        nested_if_then_else(predicate, self, other)
    }
}

impl FixedSizeListArray {
    pub fn if_else(
        &self,
        other: &FixedSizeListArray,
        predicate: &BooleanArray,
    ) -> DaftResult<FixedSizeListArray> {
        nested_if_then_else(predicate, self, other)
    }
}

impl StructArray {
    pub fn if_else(
        &self,
        other: &StructArray,
        predicate: &BooleanArray,
    ) -> DaftResult<StructArray> {
        nested_if_then_else(predicate, self, other)
    }
}
