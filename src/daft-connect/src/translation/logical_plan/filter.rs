use eyre::bail;

use crate::translation::{to_daft_expr, to_logical_plan};

pub fn filter(
    filter: spark_connect::Filter,
) -> eyre::Result<daft_logical_plan::LogicalPlanBuilder> {
    let spark_connect::Filter { input, condition } = filter;

    let Some(input) = input else {
        bail!("input is required");
    };

    let Some(condition) = condition else {
        bail!("condition is required");
    };

    let condition = to_daft_expr(&condition)?;

    let plan = to_logical_plan(*input)?;

    let plan = plan.filter(condition)?;

    Ok(plan)
}
