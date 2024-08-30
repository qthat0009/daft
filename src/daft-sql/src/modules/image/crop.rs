use daft_dsl::ExprRef;
use daft_functions::image::crop::crop;
use sqlparser::ast::FunctionArg;

use crate::{error::SQLPlannerResult, functions::SQLFunction, unsupported_sql_err};

pub struct SQLImageCrop;

impl SQLFunction for SQLImageCrop {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, bbox] => {
                let input = planner.plan_function_arg(input)?;
                let bbox = planner.plan_function_arg(bbox)?;
                Ok(crop(input, bbox))
            }
            _ => unsupported_sql_err!("Invalid arguments for image_crop: '{inputs:?}'"),
        }
    }
}
