use crate::{
    array::{
        ops::{arrow2::comparison::build_multi_array_is_equal, GroupIndicesPair, IntoGroups},
        BaseArray,
    },
    datatypes::{UInt64Array, UInt64Type},
    dsl::Expr,
    error::{DaftError, DaftResult},
    series::Series,
    table::Table,
};

use crate::array::ops::downcast::Downcastable;

impl Table {
    pub fn agg(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Table> {
        // Dispatch depending on whether we're doing groupby or just a global agg.
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[Expr]) -> DaftResult<Table> {
        self.eval_expression_list(to_agg)
    }

    pub fn agg_groupby(&self, to_agg: &[Expr], group_by: &[Expr]) -> DaftResult<Table> {
        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_series = UInt64Array::from(("", groupkey_indices)).into_series();
            groupby_table.take(&indices_as_series)?
        };
        let agg_exprs = to_agg
            .iter()
            .map(|e| match e {
                Expr::Agg(e) => Ok(e),
                _ => Err(DaftError::ValueError(format!(
                    "Trying to run non-Agg expression in Grouped Agg! {e}"
                ))),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let grouped_cols = agg_exprs
            .iter()
            .map(|e| self.eval_agg_expression(e, Some(&groupvals_indices)))
            .collect::<DaftResult<Vec<_>>>()?;
        // Combine the groupkey columns and the aggregation result columns.
        Self::from_columns([&groupkeys_table.columns[..], &grouped_cols].concat())
    }



}
