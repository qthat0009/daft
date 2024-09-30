use daft_dsl::{ExprRef, LiteralValue};
use daft_functions::numeric::{
    abs::abs,
    ceil::ceil,
    exp::exp,
    floor::floor,
    log::{ln, log, log10, log2},
    round::round,
    sign::sign,
    sqrt::sqrt,
    trigonometry::{
        arccos, arccosh, arcsin, arcsinh, arctan, arctanh, atan2, cos, cot, degrees, radians, sin,
        tan,
    },
};

use super::SQLModule;
use crate::{
    ensure,
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleNumeric;

impl SQLModule for SQLModuleNumeric {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("abs", SQLNumericExpr::Abs, "TODO: Docstring", &["TODO"]);
        parent.add_fn("ceil", SQLNumericExpr::Ceil, "TODO: Docstring", &["TODO"]);
        parent.add_fn("floor", SQLNumericExpr::Floor, "TODO: Docstring", &["TODO"]);
        parent.add_fn("sign", SQLNumericExpr::Sign, "TODO: Docstring", &["TODO"]);
        parent.add_fn("round", SQLNumericExpr::Round, "TODO: Docstring", &["TODO"]);
        parent.add_fn("sqrt", SQLNumericExpr::Sqrt, "TODO: Docstring", &["TODO"]);
        parent.add_fn("sin", SQLNumericExpr::Sin, "TODO: Docstring", &["TODO"]);
        parent.add_fn("cos", SQLNumericExpr::Cos, "TODO: Docstring", &["TODO"]);
        parent.add_fn("tan", SQLNumericExpr::Tan, "TODO: Docstring", &["TODO"]);
        parent.add_fn("cot", SQLNumericExpr::Cot, "TODO: Docstring", &["TODO"]);
        parent.add_fn("asin", SQLNumericExpr::ArcSin, "TODO: Docstring", &["TODO"]);
        parent.add_fn("acos", SQLNumericExpr::ArcCos, "TODO: Docstring", &["TODO"]);
        parent.add_fn("atan", SQLNumericExpr::ArcTan, "TODO: Docstring", &["TODO"]);
        parent.add_fn(
            "atan2",
            SQLNumericExpr::ArcTan2,
            "TODO: Docstring",
            &["TODO"],
        );
        parent.add_fn(
            "radians",
            SQLNumericExpr::Radians,
            "TODO: Docstring",
            &["TODO"],
        );
        parent.add_fn(
            "degrees",
            SQLNumericExpr::Degrees,
            "TODO: Docstring",
            &["TODO"],
        );
        parent.add_fn("log2", SQLNumericExpr::Log2, "TODO: Docstring", &["TODO"]);
        parent.add_fn("log10", SQLNumericExpr::Log10, "TODO: Docstring", &["TODO"]);
        parent.add_fn("log", SQLNumericExpr::Log, "TODO: Docstring", &["TODO"]);
        parent.add_fn("ln", SQLNumericExpr::Ln, "TODO: Docstring", &["TODO"]);
        parent.add_fn("exp", SQLNumericExpr::Exp, "TODO: Docstring", &["TODO"]);
        parent.add_fn(
            "atanh",
            SQLNumericExpr::ArcTanh,
            "TODO: Docstring",
            &["TODO"],
        );
        parent.add_fn(
            "acosh",
            SQLNumericExpr::ArcCosh,
            "TODO: Docstring",
            &["TODO"],
        );
        parent.add_fn(
            "asinh",
            SQLNumericExpr::ArcSinh,
            "TODO: Docstring",
            &["TODO"],
        );
    }
}
enum SQLNumericExpr {
    Abs,
    Ceil,
    Exp,
    Floor,
    Round,
    Sign,
    Sqrt,
    Sin,
    Cos,
    Tan,
    Cot,
    ArcSin,
    ArcCos,
    ArcTan,
    ArcTan2,
    Radians,
    Degrees,
    Log,
    Log2,
    Log10,
    Ln,
    ArcTanh,
    ArcCosh,
    ArcSinh,
}

impl SQLFunction for SQLNumericExpr {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let inputs = self.args_to_expr_unnamed(inputs, planner)?;
        to_expr(self, inputs.as_slice())
    }
}

fn to_expr(expr: &SQLNumericExpr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    match expr {
        SQLNumericExpr::Abs => {
            ensure!(args.len() == 1, "abs takes exactly one argument");
            Ok(abs(args[0].clone()))
        }
        SQLNumericExpr::Ceil => {
            ensure!(args.len() == 1, "ceil takes exactly one argument");
            Ok(ceil(args[0].clone()))
        }
        SQLNumericExpr::Floor => {
            ensure!(args.len() == 1, "floor takes exactly one argument");
            Ok(floor(args[0].clone()))
        }
        SQLNumericExpr::Sign => {
            ensure!(args.len() == 1, "sign takes exactly one argument");
            Ok(sign(args[0].clone()))
        }
        SQLNumericExpr::Round => {
            ensure!(args.len() == 2, "round takes exactly two arguments");
            let precision = match args[1].as_ref().as_literal() {
                Some(LiteralValue::Int32(i)) => *i,
                Some(LiteralValue::UInt32(u)) => *u as i32,
                Some(LiteralValue::Int64(i)) => *i as i32,
                _ => invalid_operation_err!("round precision must be an integer"),
            };
            Ok(round(args[0].clone(), precision))
        }
        SQLNumericExpr::Sqrt => {
            ensure!(args.len() == 1, "sqrt takes exactly one argument");
            Ok(sqrt(args[0].clone()))
        }
        SQLNumericExpr::Sin => {
            ensure!(args.len() == 1, "sin takes exactly one argument");
            Ok(sin(args[0].clone()))
        }
        SQLNumericExpr::Cos => {
            ensure!(args.len() == 1, "cos takes exactly one argument");
            Ok(cos(args[0].clone()))
        }
        SQLNumericExpr::Tan => {
            ensure!(args.len() == 1, "tan takes exactly one argument");
            Ok(tan(args[0].clone()))
        }
        SQLNumericExpr::Cot => {
            ensure!(args.len() == 1, "cot takes exactly one argument");
            Ok(cot(args[0].clone()))
        }
        SQLNumericExpr::ArcSin => {
            ensure!(args.len() == 1, "asin takes exactly one argument");
            Ok(arcsin(args[0].clone()))
        }
        SQLNumericExpr::ArcCos => {
            ensure!(args.len() == 1, "acos takes exactly one argument");
            Ok(arccos(args[0].clone()))
        }
        SQLNumericExpr::ArcTan => {
            ensure!(args.len() == 1, "atan takes exactly one argument");
            Ok(arctan(args[0].clone()))
        }
        SQLNumericExpr::ArcTan2 => {
            ensure!(args.len() == 2, "atan2 takes exactly two arguments");
            Ok(atan2(args[0].clone(), args[1].clone()))
        }
        SQLNumericExpr::Degrees => {
            ensure!(args.len() == 1, "degrees takes exactly one argument");
            Ok(degrees(args[0].clone()))
        }
        SQLNumericExpr::Radians => {
            ensure!(args.len() == 1, "radians takes exactly one argument");
            Ok(radians(args[0].clone()))
        }
        SQLNumericExpr::Log2 => {
            ensure!(args.len() == 1, "log2 takes exactly one argument");
            Ok(log2(args[0].clone()))
        }
        SQLNumericExpr::Log10 => {
            ensure!(args.len() == 1, "log10 takes exactly one argument");
            Ok(log10(args[0].clone()))
        }
        SQLNumericExpr::Ln => {
            ensure!(args.len() == 1, "ln takes exactly one argument");
            Ok(ln(args[0].clone()))
        }
        SQLNumericExpr::Log => {
            ensure!(args.len() == 2, "log takes exactly two arguments");
            let base = args[1]
                .as_literal()
                .and_then(|lit| match lit {
                    LiteralValue::Float64(f) => Some(*f),
                    LiteralValue::Int32(i) => Some(*i as f64),
                    LiteralValue::UInt32(u) => Some(*u as f64),
                    LiteralValue::Int64(i) => Some(*i as f64),
                    LiteralValue::UInt64(u) => Some(*u as f64),
                    _ => None,
                })
                .ok_or_else(|| PlannerError::InvalidOperation {
                    message: "log base must be a float or a number".to_string(),
                })?;

            Ok(log(args[0].clone(), base))
        }
        SQLNumericExpr::Exp => {
            ensure!(args.len() == 1, "exp takes exactly one argument");
            Ok(exp(args[0].clone()))
        }
        SQLNumericExpr::ArcTanh => {
            ensure!(args.len() == 1, "atanh takes exactly one argument");
            Ok(arctanh(args[0].clone()))
        }
        SQLNumericExpr::ArcCosh => {
            ensure!(args.len() == 1, "acosh takes exactly one argument");
            Ok(arccosh(args[0].clone()))
        }
        SQLNumericExpr::ArcSinh => {
            ensure!(args.len() == 1, "asinh takes exactly one argument");
            Ok(arcsinh(args[0].clone()))
        }
    }
}
