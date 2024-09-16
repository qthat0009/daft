use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::super::FunctionEvaluator;
use crate::{functions::FunctionExpr, ExprRef};

pub(super) struct RpadEvaluator {}

impl FunctionEvaluator for RpadEvaluator {
    fn fn_name(&self) -> &'static str {
        "rpad"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        match inputs {
            [data, length, pad] => {
                let data = data.to_field(schema)?;
                let length = length.to_field(schema)?;
                let pad = pad.to_field(schema)?;
                if data.dtype == DataType::Utf8
                    && length.dtype.is_integer()
                    && pad.dtype == DataType::Utf8
                {
                    Ok(Field::new(data.name, DataType::Utf8))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to rpad to be utf8, integer and utf8, but received {}, {}, and {}", data.dtype, length.dtype, pad.dtype
                    )))
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        match inputs {
            [data, length, pad] => data.utf8_rpad(length, pad),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}
