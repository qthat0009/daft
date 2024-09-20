use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::DataType,
    series::{array_impl::IntoSeries, Series},
};

impl Series {
    pub fn round(&self, decimal: i32) -> DaftResult<Self> {
        match self.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(self.clone()),
            DataType::Float32 => Ok(self.f32().unwrap().round(decimal)?.into_series()),
            DataType::Float64 => Ok(self.f64().unwrap().round(decimal)?.into_series()),
            dt => Err(DaftError::TypeError(format!(
                "round not implemented for {}",
                dt
            ))),
        }
    }
}
