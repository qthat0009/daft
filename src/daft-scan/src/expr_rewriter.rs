use std::collections::HashMap;

use common_error::DaftResult;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode, VisitRecursion},
    functions::partitioning,
    null_lit,
    optimization::split_conjuction,
    Expr, Operator,
};

use crate::{PartitionField, PartitionTransform};

fn unalias(expr: Expr) -> DaftResult<Expr> {
    expr.transform(&|e| {
        if let Expr::Alias(e, _) = e {
            Ok(Transformed::Yes(e.as_ref().clone()))
        } else {
            Ok(Transformed::No(e))
        }
    })
}

fn apply_partitioning_expr(expr: Expr, pfield: &PartitionField) -> Option<Expr> {
    use PartitionTransform::*;
    match pfield.transform {
        Some(Identity) => Some(
            pfield
                .source_field
                .as_ref()
                .map(|s| expr.cast(&s.dtype))
                .unwrap_or(expr),
        ),
        Some(Year) => Some(partitioning::years(expr)),
        Some(Month) => Some(partitioning::months(expr)),
        Some(Day) => Some(partitioning::days(expr)),
        Some(Hour) => Some(partitioning::hours(expr)),
        Some(Void) => Some(null_lit()),
        Some(IcebergBucket(n)) => Some(partitioning::iceberg_bucket(
            expr.cast(&pfield.source_field.as_ref().unwrap().dtype),
            n as i32,
        )),
        Some(IcebergTruncate(w)) => Some(partitioning::iceberg_truncate(
            expr.cast(&pfield.source_field.as_ref().unwrap().dtype),
            w as i64,
        )),
        _ => None,
    }
}

/// Grouping of clauses in a conjunctive predicate around partitioning semantics.
pub struct PredicateGroups {
    pub identity_partition_filter: Vec<Expr>,
    pub non_identity_partition_filter: Vec<Expr>,
    pub partition_and_data_filter: Vec<Expr>,
    pub data_only_filter: Vec<Expr>,
}

impl PredicateGroups {
    pub fn new(
        identity_partition_filter: Vec<Expr>,
        non_identity_partition_filter: Vec<Expr>,
        partition_and_data_filter: Vec<Expr>,
        data_only_filter: Vec<Expr>,
    ) -> Self {
        Self {
            identity_partition_filter,
            non_identity_partition_filter,
            partition_and_data_filter,
            data_only_filter,
        }
    }

    pub fn from_data_only(data_only_filter: Vec<Expr>) -> Self {
        Self::new(vec![], vec![], vec![], data_only_filter)
    }
}

pub fn rewrite_predicate_for_partitioning(
    predicate: Expr,
    pfields: &[PartitionField],
) -> DaftResult<PredicateGroups> {
    if pfields.is_empty() {
        return Ok(PredicateGroups::from_data_only(vec![predicate]));
    }

    let predicate = unalias(predicate)?;

    let source_to_pfield = {
        let mut map = HashMap::with_capacity(pfields.len());
        for pf in pfields.iter() {
            if let Some(ref source_field) = pf.source_field {
                let prev_value = map.insert(source_field.name.as_str(), pf);
                if let Some(prev_value) = prev_value {
                    return Err(common_error::DaftError::ValueError(format!("Duplicate Partitioning Columns found on same source field: {source_field}\n1: {prev_value}\n2: {pf}")));
                }
            }
        }
        map
    };

    let with_part_cols = predicate.transform(&|expr| {
        use Operator::*;
        match expr {
            // Binary Op for Eq
            // All transforms should work as is
            Expr::BinaryOp {
                op: Eq,
                ref left, ref right } => {
                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_equals() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: Eq, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_equals() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: Eq, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            // Binary Op for NotEq
            // Should only work for Identity
            Expr::BinaryOp {
                op: NotEq,
                ref left, ref right } => {
                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_not_equals() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: NotEq, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_not_equals() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: NotEq, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            // Binary Op for Lt | LtEq | Gt | GtEq
            // we need to relax Lt and LtEq and only allow certain Transforms
            Expr::BinaryOp {
                op,
                ref left, ref right } if matches!(op, Lt | LtEq | Gt | GtEq)=> {
                let relaxed_op = match op {
                    Lt | LtEq => LtEq,
                    Gt | GtEq => GtEq,
                    _ => unreachable!("this branch only supports Lt | LtEq | Gt | GtEq")
                };

                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_comparison() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: relaxed_op, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_comparison() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: relaxed_op, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },

            Expr::IsNull(ref expr) if let Expr::Column(col_name) = expr.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) => {
                Ok(Transformed::Yes(Expr::IsNull(col(pfield.field.name.as_str()).into())))
            },
            Expr::NotNull(ref expr) if let Expr::Column(col_name) = expr.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) => {
                Ok(Transformed::Yes(Expr::NotNull(col(pfield.field.name.as_str()).into())))
            },
            _ => Ok(Transformed::No(expr))
        }
    })?;

    let pfields_map: HashMap<&str, &PartitionField> = pfields
        .iter()
        .map(|pfield| (pfield.field.name.as_str(), pfield))
        .collect();

    let split = split_conjuction(&with_part_cols);
    // Predicates that only involve identity transformations on partition columns.
    let mut part_preds_identity = vec![];
    // Predicates that only reference partition columns, but involve non-identity transformations.
    let mut part_preds_non_identity = vec![];
    // Predicates that reference both partition columns and data columns.
    let mut part_and_data_preds = vec![];
    // Predicates that only reference data columns (no partition column references).
    let mut non_part_preds = vec![];
    for e in split.into_iter() {
        let mut all_part_keys = true;
        let mut all_identity_part_keys = true;
        let mut at_least_one_part_key = false;
        e.apply(&mut |e| {
            if let Expr::Column(col_name) = e {
                if let Some(pfield) = pfields_map.get(col_name.as_ref()) {
                    at_least_one_part_key = true;
                    if !matches!(pfield.transform, Some(PartitionTransform::Identity) | None) {
                        all_identity_part_keys = false;
                    }
                } else {
                    all_part_keys = false;
                    all_identity_part_keys = false;
                }
            }
            Ok(VisitRecursion::Continue)
        })
        .unwrap();

        // Push to appropriate vec.
        if all_identity_part_keys {
            part_preds_identity.push(e.clone());
        } else if all_part_keys {
            part_preds_non_identity.push(e.clone());
        } else if at_least_one_part_key {
            part_and_data_preds.push(e.clone());
        } else {
            non_part_preds.push(e.clone());
        }
    }
    Ok(PredicateGroups::new(
        part_preds_identity,
        part_preds_non_identity,
        part_and_data_preds,
        non_part_preds,
    ))
}
