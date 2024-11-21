use eyre::bail;
use spark_connect::{expression::ExprType, Expression};

use crate::translation::{to_daft_expr, to_logical_plan};

pub fn with_columns(
    with_columns: spark_connect::WithColumns,
) -> eyre::Result<daft_logical_plan::LogicalPlanBuilder> {
    let spark_connect::WithColumns { input, aliases } = with_columns;

    let Some(input) = input else {
        bail!("input is required");
    };

    let plan = to_logical_plan(*input)?;

    let daft_exprs: Vec<_> = aliases
        .into_iter()
        .map(|alias| {
            let expression = Expression {
                common: None,
                expr_type: Some(ExprType::Alias(Box::new(alias))),
            };

            to_daft_expr(&expression)
        })
        .try_collect()?;

    let plan = plan.with_columns(daft_exprs)?;

    Ok(plan)
}
