use eyre::bail;

use crate::translation::to_logical_plan;

pub fn drop(drop: spark_connect::Drop) -> eyre::Result<daft_logical_plan::LogicalPlanBuilder> {
    let spark_connect::Drop {
        input,
        columns,
        column_names,
    } = drop;

    let Some(input) = input else {
        bail!("input is required");
    };

    if !columns.is_empty() {
        bail!("columns is not supported; use column_names instead");
    }

    let plan = to_logical_plan(*input)?;

    // Get all column names from the schema
    let all_columns = plan.schema().names();

    // Create a set of columns to drop for efficient lookup
    let columns_to_drop: std::collections::HashSet<_> = column_names.iter().collect();

    // Create expressions for all columns except the ones being dropped
    let to_select = all_columns
        .iter()
        .filter(|col_name| !columns_to_drop.contains(*col_name))
        .map(|col_name| daft_dsl::col(col_name.clone()))
        .collect();

    // Use select to keep only the columns we want
    let plan = plan.select(to_select)?;

    Ok(plan)
}
