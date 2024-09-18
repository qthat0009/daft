use common_error::DaftResult;
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Exp {}

#[typetag::serde]
impl ScalarUDF for Exp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "exp"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field_single_numeric(self, inputs, schema)
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, Series::exp)
    }
}

pub fn exp(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Exp {}, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

use super::{evaluate_single_numeric, to_field_single_numeric};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "exp")]
pub fn py_exp(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(exp(expr.into()).into())
}
