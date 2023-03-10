mod apply;
mod arange;
mod arithmetic;
mod arrow2;
mod broadcast;
mod cast;
mod comparison;
mod count;
mod downcast;
mod filter;
mod full;
mod hash;
mod len;
mod numeric_agg;
mod pairwise;
mod sort;
mod take;

pub trait DaftCompare<Rhs> {
    type Output;

    /// equality.
    fn equal(&self, rhs: Rhs) -> Self::Output;

    /// inequality.
    fn not_equal(&self, rhs: Rhs) -> Self::Output;

    /// Greater than
    fn gt(&self, rhs: Rhs) -> Self::Output;

    /// Greater than or equal
    fn gte(&self, rhs: Rhs) -> Self::Output;

    /// Less than
    fn lt(&self, rhs: Rhs) -> Self::Output;

    /// Less than or equal
    fn lte(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftLogical<Rhs> {
    type Output;

    /// and.
    fn and(&self, rhs: Rhs) -> Self::Output;

    /// or.
    fn or(&self, rhs: Rhs) -> Self::Output;

    /// xor.
    fn xor(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftCountAggable {
    type Output;
    fn count(&self) -> Self::Output;
}

pub trait DaftNumericAggable {
    type SumOutput;
    type MeanOutput;
    fn sum(&self) -> Self::SumOutput;
    fn mean(&self) -> Self::MeanOutput;
}

pub trait DaftCompareAggable {
    type Output;
    fn min(&self) -> Self::Output;
    fn max(&self) -> Self::Output;
}
