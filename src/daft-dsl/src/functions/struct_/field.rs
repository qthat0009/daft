use crate::Expr;
use daft_core::{
    datatypes::{DataType, Field},
    schema::Schema,
    series::Series,
};

use crate::functions::FunctionExpr;
use common_error::{DaftError, DaftResult};

use super::{super::FunctionEvaluator, StructExpr};

pub(super) struct FieldEvaluator {}

impl FunctionEvaluator for FieldEvaluator {
    fn fn_name(&self) -> &'static str {
        "field"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema, expr: &Expr) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;

                match input_field.dtype {
                    DataType::Struct(fields) => {
                        let name = match expr {
                            Expr::Function {
                                func: FunctionExpr::Struct(StructExpr::Field(name)),
                                inputs: _,
                            } => name,
                            _ => panic!("Expected Struct Field Expr, got {expr}"),
                        };

                        for f in &fields {
                            if f.name == *name {
                                return Ok(Field::new(name, f.dtype.clone()));
                            }
                        }

                        Err(DaftError::FieldNotFound(format!(
                            "Field {} not found in schema: {:?}",
                            name,
                            fields
                                .iter()
                                .map(|f| f.name.clone())
                                .collect::<Vec<String>>()
                        )))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected input to be a list type, received: {}",
                        input_field.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series], expr: &Expr) -> DaftResult<Series> {
        match inputs {
            [input] => {
                let name = match expr {
                    Expr::Function {
                        func: FunctionExpr::Struct(StructExpr::Field(name)),
                        inputs: _,
                    } => name,
                    _ => panic!("Expected Struct Field Expr, got {expr}"),
                };

                input.struct_field(name)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}
