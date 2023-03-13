use crate::{
    datatypes::dtype_utils,
    datatypes::DataType,
    datatypes::Field,
    dsl::{functions::FunctionEvaluator, lit},
    error::{DaftError, DaftResult},
    schema::Schema,
    utils::supertype::try_get_supertype,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result},
    sync::Arc,
};

use super::functions::FunctionExpr;

type ExprRef = Arc<Expr>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Alias(ExprRef, Arc<str>),
    Agg(AggExpr),
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },
    Cast(ExprRef, DataType),
    Column(Arc<str>),
    Function {
        func: FunctionExpr,
        inputs: Vec<Expr>,
    },
    Literal(lit::LiteralValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggExpr {
    Sum(ExprRef),
}

pub fn col<S: Into<Arc<str>>>(name: S) -> Expr {
    Expr::Column(name.into())
}

pub fn binary_op(op: Operator, left: &Expr, right: &Expr) -> Expr {
    Expr::BinaryOp {
        op,
        left: left.clone().into(),
        right: right.clone().into(),
    }
}

impl AggExpr {
    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use AggExpr::*;
        match self {
            Sum(expr) => {
                let field = expr.to_field(schema)?;
                Ok(Field::new(
                    field.name.as_str(),
                    match &field.dtype {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            DataType::Int64
                        }
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => DataType::UInt64,
                        DataType::Float32 => DataType::Float32,
                        DataType::Float64 => DataType::Float64,
                        _other => {
                            return Err(unary_resolving_type_error(
                                "sum",
                                "input to be numeric",
                                expr,
                                &field,
                            ))
                        }
                    },
                ))
            }
        }
    }
}

impl Expr {
    pub fn alias<S: Into<Arc<str>>>(&self, name: S) -> Self {
        Expr::Alias(self.clone().into(), name.into())
    }

    pub fn cast(&self, dtype: &DataType) -> Self {
        Expr::Cast(self.clone().into(), dtype.clone())
    }

    pub fn sum(&self) -> Self {
        Expr::Agg(AggExpr::Sum(self.clone().into()))
    }

    pub fn and(&self, other: &Self) -> Self {
        binary_op(Operator::And, self, other)
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;
        match self {
            Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Agg(agg_expr) => agg_expr.to_field(schema),
            Cast(expr, dtype) => Ok(Field::new(expr.name()?, dtype.clone())),
            Column(name) => Ok(schema.get_field(name).cloned()?),
            Literal(value) => Ok(Field::new("literal", value.get_type())),
            Function { func, inputs } => func.to_field(inputs.as_slice(), schema),
            BinaryOp { op, left, right } => {
                let left_field = left.to_field(schema)?;
                let right_field = right.to_field(schema)?;

                match op {
                    // Logical operations
                    Operator::And | Operator::Or | Operator::Xor => {
                        if left_field.dtype != DataType::Boolean
                            || right_field.dtype != DataType::Boolean
                        {
                            return Err(binary_resolving_type_error(
                                op,
                                "all boolean arguments",
                                left,
                                right,
                                &left_field,
                                &right_field,
                            ));
                        }
                        Ok(Field::new(left_field.name.as_str(), DataType::Boolean))
                    }

                    // Comparison operations
                    Operator::Lt
                    | Operator::Gt
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::LtEq
                    | Operator::GtEq => {
                        match try_get_supertype(&left_field.dtype, &right_field.dtype) {
                            Ok(_) => Ok(Field::new(
                                left.to_field(schema)?.name.as_str(),
                                DataType::Boolean,
                            )),
                            Err(_) => Err(binary_resolving_type_error(op, "left and right arguments to be castable to the same supertype for comparison", left, right, &left_field, &right_field)),
                        }
                    }

                    // Plus operation: special-cased as it has semantic meaning for string types
                    Operator::Plus => {
                        match try_get_supertype(&left_field.dtype, &right_field.dtype) {
                            Ok(supertype) => Ok(Field::new(
                                left.to_field(schema)?.name.as_str(),
                                supertype,
                            )),
                            Err(_) if left_field.dtype == DataType::Utf8 => Err(binary_resolving_type_error(op, "right argument to be castable to string for a string concat since left argument resolves to a string", left, right, &left_field, &right_field)),
                            Err(_) if right_field.dtype == DataType::Utf8 => Err(binary_resolving_type_error(op, "left argument to be castable to string for a string concat since right argument resolves to a string", left, right, &left_field, &right_field)),
                            Err(_) => Err(binary_resolving_type_error(op, "left and right arguments to both be numeric", left, right, &left_field, &right_field)),
                        }
                    }

                    // True divide operation
                    Operator::TrueDivide => {
                        if !dtype_utils::is_castable(&left_field.dtype, &DataType::Float64)?
                            || !dtype_utils::is_castable(&left_field.dtype, &DataType::Float64)?
                        {
                            return Err(binary_resolving_type_error(
                                op,
                                format!(
                                    "left and right arguments to both be castable to {}",
                                    DataType::Float64
                                )
                                .as_str(),
                                left,
                                right,
                                &left_field,
                                &right_field,
                            ));
                        }
                        Ok(Field::new(left_field.name.as_str(), DataType::Float64))
                    }

                    // Regular arithmetic operations
                    Operator::Minus
                    | Operator::Multiply
                    | Operator::Modulus
                    | Operator::FloorDivide => {
                        if !dtype_utils::is_numeric(&left_field.dtype)
                            || !dtype_utils::is_numeric(&right_field.dtype)
                        {
                            return Err(binary_resolving_type_error(
                                op,
                                "left and right arguments to both be numeric",
                                left,
                                right,
                                &left_field,
                                &right_field,
                            ));
                        }
                        Ok(Field::new(
                            left_field.name.as_str(),
                            try_get_supertype(&left_field.dtype, &right_field.dtype)?,
                        ))
                    }
                }
            }
        }
    }

    pub fn name(&self) -> DaftResult<&str> {
        use AggExpr::*;
        use Expr::*;
        match self {
            Alias(.., name) => Ok(name.as_ref()),
            Agg(agg_expr) => match agg_expr {
                Sum(expr) => expr.name(),
            },
            Cast(expr, ..) => expr.name(),
            Column(name) => Ok(name.as_ref()),
            Literal(..) => Ok("literal"),
            Function { func: _, inputs } => inputs.first().unwrap().name(),
            BinaryOp {
                op: _,
                left,
                right: _,
            } => left.name(),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DaftResult<DataType> {
        Ok(self.to_field(schema)?.dtype)
    }
}

impl Display for Expr {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        use AggExpr::*;
        use Expr::*;
        match self {
            Alias(expr, name) => write!(f, "{expr} AS {name}"),
            Agg(agg_expr) => match agg_expr {
                Sum(expr) => write!(f, "sum({expr})"),
            },
            BinaryOp { op, left, right } => {
                let write_out_expr = |f: &mut Formatter, input: &Expr| match input {
                    Alias(e, _) => write!(f, "{e}"),
                    BinaryOp { .. } => write!(f, "[{input}]"),
                    _ => write!(f, "{input}"),
                };

                write_out_expr(f, left)?;
                write!(f, " {op} ")?;
                write_out_expr(f, right)?;
                Ok(())
            }
            Cast(expr, dtype) => write!(f, "cast({expr} AS {dtype})"),
            Column(name) => write!(f, "col({name})"),
            Literal(val) => write!(f, "lit({val})"),
            Function { func, inputs } => {
                write!(f, "{}(", func.fn_name())?;
                for i in 0..(inputs.len() - 1) {
                    write!(f, "{}, ", inputs.get(i).unwrap())?;
                }
                if !inputs.is_empty() {
                    write!(f, "{})", inputs.last().unwrap())?;
                }
                Ok(())
            }
        }
    }
}

/// Based on Polars first class operators: https://github.com/pola-rs/polars/blob/master/polars/polars-lazy/polars-plan/src/dsl/expr.rs
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    TrueDivide,
    FloorDivide,
    Modulus,
    And,
    Or,
    Xor,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Operator::*;
        let tkn = match self {
            Eq => "==",
            NotEq => "!=",
            Lt => "<",
            LtEq => "<=",
            Gt => ">",
            GtEq => ">=",
            Plus => "+",
            Minus => "-",
            Multiply => "*",
            TrueDivide => "/",
            FloorDivide => "//",
            Modulus => "%",
            And => "&",
            Or => "|",
            Xor => "^",
        };
        write!(f, "{tkn}")
    }
}

impl Operator {
    #![allow(dead_code)]
    pub(crate) fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::NotEq
                | Self::Lt
                | Self::LtEq
                | Self::Gt
                | Self::GtEq
                | Self::And
                | Self::Or
                | Self::Xor
        )
    }

    pub(crate) fn is_arithmetic(&self) -> bool {
        !(self.is_comparison())
    }
}

// Helper method that standardizes the error messages when "unary" expressions fail during type-resolution against a schema
fn unary_resolving_type_error(
    op_display_name: &str,
    expects: &str,
    expr: &Expr,
    field: &Field,
) -> DaftError {
    let name = field.name.as_str();
    DaftError::TypeError(format!(
        "{op_display_name} expects {expects}, but failed type resolution: {op_display_name}(`{name}`)

        `{name}` resolves to a {}: {expr}", field.dtype
    ))
}

// Helper method that standardizes the error messages when "binary" expressions fail during type-resolution against a schema
fn binary_resolving_type_error(
    op: &Operator,
    expects: &str,
    left_expr: &Expr,
    right_expr: &Expr,
    left_field: &Field,
    right_field: &Field,
) -> DaftError {
    let left_name = left_field.name.as_str();
    let right_name = right_field.name.as_str();
    DaftError::TypeError(format!(
        "{:?} expects {expects}, but failed type resolution: `{left_name}` {op} `{right_name}`

        `{left_name}` resolves to a {}: {left_expr}
        `{right_name}` resolves to a {}: {right_expr}",
        op, left_field.dtype, right_field.dtype
    ))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dsl::lit;
    #[test]
    fn check_comparision_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Lt,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Boolean);
        Ok(())
    }

    #[test]
    fn check_alias_type() -> DaftResult<()> {
        let a = col("a");
        let b = a.alias("b");
        match b {
            Expr::Alias(..) => Ok(()),
            other => Err(crate::error::DaftError::ValueError(format!(
                "expected expression to be a alias, got {other:?}"
            ))),
        }
    }

    #[test]
    fn check_arithmetic_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = lit(10.);
        let y = lit(12);

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }

    #[test]
    fn check_arithmetic_type_with_columns() -> DaftResult<()> {
        let x = col("x");
        let y = col("y");
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64),
            Field::new("y", DataType::Int64),
        ]);

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = col("x");
        let y = col("y");

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }
}
