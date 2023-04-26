use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::{BooleanArray, BooleanType, DataType},
    error::{DaftError, DaftResult},
    series::Series,
    with_match_comparable_daft_types,
};

use super::match_types_on_series;
#[cfg(feature = "python")]
use super::py_binary_op;

macro_rules! impl_compare {
    ($fname:ident, $pycmp:expr) => {
        fn $fname(&self, rhs: &Series) -> Self::Output {
            let (lhs, rhs) = match_types_on_series(self, rhs)?;

            #[cfg(feature = "python")]
            if lhs.data_type() == &DataType::Python {
                return py_binary_op!(lhs, rhs, $pycmp)
                    .downcast::<BooleanType>()
                    .cloned();
            }

            let lhs = lhs.as_physical()?;
            let rhs = rhs.as_physical()?;

            with_match_comparable_daft_types!(lhs.data_type(), |$T| {
                let lhs = lhs.downcast::<$T>()?;
                let rhs = rhs.downcast::<$T>()?;
                lhs.$fname(rhs)
            })
        }
    };
}

impl DaftCompare<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    impl_compare!(equal, "==");
    impl_compare!(not_equal, "!=");
    impl_compare!(lt, "<");
    impl_compare!(lte, "<=");
    impl_compare!(gt, ">");
    impl_compare!(gte, ">=");
}

macro_rules! impl_logical {
    ($fname:ident, $pyop:expr) => {
        fn $fname(&self, other: &Series) -> Self::Output {
            let (lhs, rhs) = match_types_on_series(self, other)?;

            #[cfg(feature = "python")]
            if lhs.data_type() == &DataType::Python {
                return py_binary_op!(lhs, rhs, $pyop).downcast::<BooleanType>().cloned();
            }

            if lhs.data_type() != &DataType::Boolean {
                return Err(DaftError::TypeError(format!(
                    "Can only perform logical operations on boolean supertype, but got left series {} and right series {} with supertype {}",
                    self.field(),
                    other.field(),
                    lhs.data_type(),
                )));
            }
            self.downcast::<BooleanType>()?
                .$fname(rhs.downcast::<BooleanType>()?)
        }
    };
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    impl_logical!(and, "&");
    impl_logical!(or, "|");
    impl_logical!(xor, "^");
}
