use eyre::{bail, Context};
use spark_connect::set_operation::SetOpType;
use tracing::warn;

use crate::translation::to_logical_plan;

pub fn set_op(
    set_op: spark_connect::SetOperation,
) -> eyre::Result<daft_logical_plan::LogicalPlanBuilder> {
    let spark_connect::SetOperation {
        left_input,
        right_input,
        set_op_type,
        is_all,
        by_name,
        allow_missing_columns,
    } = set_op;

    let Some(left_input) = left_input else {
        bail!("Left input is required");
    };

    let Some(right_input) = right_input else {
        bail!("Right input is required");
    };

    let set_op = SetOpType::try_from(set_op_type)
        .wrap_err_with(|| format!("Invalid set operation type: {set_op_type}"))?;

    if let Some(by_name) = by_name {
        warn!("Ignoring by_name: {by_name}");
    }

    if let Some(allow_missing_columns) = allow_missing_columns {
        warn!("Ignoring allow_missing_columns: {allow_missing_columns}");
    }

    let left = to_logical_plan(*left_input)?;
    let right = to_logical_plan(*right_input)?;

    let is_all = is_all.unwrap_or(false);

    match set_op {
        SetOpType::Unspecified => {
            bail!("Unspecified set operation is not supported");
        }
        SetOpType::Intersect => left
            .intersect(&right, is_all)
            .wrap_err("Failed to apply intersect to logical plan"),
        SetOpType::Union => left
            .union(&right, is_all)
            .wrap_err("Failed to apply union to logical plan"),
        SetOpType::Except => {
            bail!("Except set operation is not supported");
        }
    }
}
