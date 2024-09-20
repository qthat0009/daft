use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;

use super::{super::FunctionEvaluator, NumericExpr};
use crate::{functions::FunctionExpr, ExprRef};

pub(super) struct RoundEvaluator {}

impl FunctionEvaluator for RoundEvaluator {
    fn fn_name(&self) -> &'static str {
        "round"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        if !field.dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "Expected input to round to be numeric, got {}",
                field.dtype
            )));
        }
        Ok(field)
    }

    fn evaluate(&self, inputs: &[Series], expr: &FunctionExpr) -> DaftResult<Series> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let decimal = match expr {
            FunctionExpr::Numeric(NumericExpr::Round(index)) => index,
            _ => panic!("Expected Round Expr, got {expr}"),
        };
        inputs.first().unwrap().round(*decimal)
    }
}
