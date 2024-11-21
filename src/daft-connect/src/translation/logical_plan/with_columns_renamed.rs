use daft_dsl::col;
use eyre::{bail, Context};

pub fn with_columns_renamed(
    with_columns_renamed: spark_connect::WithColumnsRenamed,
) -> eyre::Result<daft_logical_plan::LogicalPlanBuilder> {
    let spark_connect::WithColumnsRenamed {
        input,
        rename_columns_map,
        renames,
    } = with_columns_renamed;

    let Some(input) = input else {
        bail!("Input is required");
    };

    let plan = crate::translation::to_logical_plan(*input)?;

    // todo: do we want to implement this directly into daft?

    // Convert the rename mappings into expressions
    let rename_exprs = if !rename_columns_map.is_empty() {
        // Use rename_columns_map if provided (legacy format)
        rename_columns_map
            .into_iter()
            .map(|(old_name, new_name)| col(old_name.as_str()).alias(new_name.as_str()))
            .collect()
    } else {
        // Use renames if provided (new format)
        renames
            .into_iter()
            .map(|rename| col(rename.col_name.as_str()).alias(rename.new_col_name.as_str()))
            .collect()
    };

    // Apply the rename expressions to the plan
    let plan = plan
        .select(rename_exprs)
        .wrap_err("Failed to apply rename expressions to logical plan")?;

    Ok(plan)
}
