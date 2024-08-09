use std::{collections::HashSet, iter, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRewriter};
use daft_dsl::{
    functions::{
        python::{PythonUDF, StatefulPythonUDF},
        FunctionExpr,
    },
    optimization::requires_computation,
    Expr, ExprRef,
};
use itertools::Itertools;

use crate::{
    logical_ops::{ActorPoolProject, Project},
    LogicalPlan,
};

use super::{ApplyOrder, OptimizerRule, Transformed};

#[derive(Default, Debug)]
pub struct SplitActorPoolProjects {}

/// Implement SplitActorPoolProjects as an OptimizerRule which will:
///
/// 1. Go top-down from the root of the LogicalPlan
/// 2. Whenever it sees a Project with StatefulUDF(s), it will iteratively split it into a chain of sequences like so:
///     `(Project -> ActorPoolProject -> ActorPoolProject -> ...) -> (Project -> ActorPoolProject -> ActorPoolProject -> ...) -> ...`
///
/// The general idea behind the splitting is that this is a greedy algorithm which will:
/// * Skim off the top of every expression in the projection to generate "stages" for every expression
/// * Generate Project/ActorPoolProject nodes based on those stages (coalesce non-stateful stages into a Project, and run the stateful stages as ActorPoolProjects sequentially)
/// * Loop until every expression in the projection has been exhausted
///
/// For a given expression tree, skimming a Stage off the top entails:
/// 1. Iterate down the root of the tree, stopping whenever we encounter a StatefulUDF expression
/// 2. If the current stage is rooted at a StatefulUDF expression, then replace its children with Expr::Columns and return the StatefulUDF expression as its own stage
/// 3. Otherwise, the current stage is not a StatefulUDF expression: chop off any StatefulUDF children and replace them with Expr::Columns
impl OptimizerRule for SplitActorPoolProjects {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // TODO: Figure out num_actors! How do we propagate this correctly?
        let num_actors = 1;

        match plan.as_ref() {
            LogicalPlan::Project(projection) => {
                try_optimize_project(projection, plan.clone(), num_actors, 0)
            }
            // TODO: Figure out how to split other nodes as well such as Filter, Agg etc
            _ => Ok(Transformed::No(plan)),
        }
    }
}

struct SplitExprByStatefulUDF {
    // Initialized to True, but once we encounter non-aliases this will be set to false
    is_parsing_stateful_udf: bool,
    next_exprs: Vec<ExprRef>,
    stage_id: usize,
    monotonically_increasing_expr_identifier: usize,
}

impl SplitExprByStatefulUDF {
    fn new(stage_id: usize) -> Self {
        Self {
            is_parsing_stateful_udf: true,
            next_exprs: Vec::new(),
            stage_id,
            monotonically_increasing_expr_identifier: 0,
        }
    }
}

impl TreeNodeRewriter for SplitExprByStatefulUDF {
    type Node = ExprRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            // Encountered alias: keep going if we are ignoring aliases
            Expr::Alias { .. } if self.is_parsing_stateful_udf => {
                Ok(common_treenode::Transformed::no(node))
            }
            // Encountered stateful UDF: chop off all children and add to self.next_exprs
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF { .. })),
                inputs,
            } => {
                assert!(self.is_parsing_stateful_udf, "SplitExprByStatefulUDF.is_parsing_stateful_udf should be True if we encounter a stateful UDF expression");

                let new_inputs = inputs.iter().map(|e| {
                    if requires_computation(e.as_ref()) {
                        // Give the new child a deterministic name
                        let intermediate_expr_name = format!(
                            "__SplitExprByStatefulUDF_{}-{}_stateful_child__",
                            self.stage_id, self.monotonically_increasing_expr_identifier
                        );
                        self.monotonically_increasing_expr_identifier += 1;

                        // Truncate the child and push it onto the stack to indicate that it needs computation in a different stage
                        self.next_exprs
                            .push(e.clone().alias(intermediate_expr_name.as_str()));
                        Expr::Column(intermediate_expr_name.as_str().into()).arced()
                    } else {
                        e.clone()
                    }
                });
                let new_truncated_node = node.with_new_children(new_inputs.collect()).arced();

                Ok(common_treenode::Transformed::new(
                    new_truncated_node,
                    true,
                    common_treenode::TreeNodeRecursion::Jump,
                ))
            }
            Expr::Column(_) => {
                self.next_exprs.push(node.clone());
                Ok(common_treenode::Transformed::no(node))
            }
            expr => {
                // Indicate that we are now parsing a stateless expression tree
                self.is_parsing_stateful_udf = false;

                // None of the direct children are stateful UDFs, so we keep going
                if node.children().iter().all(|e| {
                    !matches!(
                        e.as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(PythonUDF::Stateful(
                                StatefulPythonUDF { .. }
                            )),
                            ..
                        }
                    )
                }) {
                    return Ok(common_treenode::Transformed::no(node));
                }

                // If any children are stateful UDFs, we truncate
                let inputs = expr.children();
                let new_inputs = inputs.iter().map(|e| {
                    if matches!(
                        e.as_ref(),
                        Expr::Function {
                            func: FunctionExpr::Python(PythonUDF::Stateful(
                                StatefulPythonUDF { .. }
                            )),
                            ..
                        }
                    ) {
                        let intermediate_expr_name = format!(
                            "__SplitExprByStatefulUDF_{}-{}_stateful__",
                            self.stage_id, self.monotonically_increasing_expr_identifier
                        );
                        self.monotonically_increasing_expr_identifier += 1;
                        self.next_exprs
                            .push(e.clone().alias(intermediate_expr_name.as_str()));
                        Expr::Column(intermediate_expr_name.as_str().into()).arced()
                    } else {
                        e.clone()
                    }
                });
                let new_truncated_node = node.with_new_children(new_inputs.collect()).arced();

                Ok(common_treenode::Transformed::yes(new_truncated_node))
            }
        }
    }
}

fn try_optimize_project(
    projection: &Project,
    plan: Arc<LogicalPlan>,
    num_actors: usize,
    recursive_count: usize,
) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
    // Simple common case: no stateful UDFs at all and we have no transformations
    let has_stateful_udfs = projection.projection.iter().any(has_stateful_udf);
    if !has_stateful_udfs {
        return Ok(Transformed::No(plan));
    }

    let (remaining, next_stages): (Vec<ExprRef>, Vec<ExprRef>) = {
        let mut remaining = Vec::new();
        let mut next_stages = Vec::new();
        for expr in projection.projection.iter() {
            let mut rewriter = SplitExprByStatefulUDF::new(recursive_count);
            let root = expr.clone().rewrite(&mut rewriter)?.data;
            next_stages.push(root);
            remaining.extend(rewriter.next_exprs);
        }
        (remaining, next_stages)
    };

    println!(
        "Optimizing: {}",
        projection
            .projection
            .iter()
            .map(|e| e.as_ref().to_string())
            .join(", ")
    );
    println!(
        "Remaining: {}",
        remaining.iter().map(|e| e.as_ref().to_string()).join(", ")
    );
    println!(
        "Next Stages: {}",
        next_stages
            .iter()
            .map(|e| e.as_ref().to_string())
            .join(", ")
    );

    // Start building the tree back up starting from the children
    let new_plan_child = if remaining
        .iter()
        .all(|e| matches!(e.as_ref(), Expr::Column(_)))
    {
        // Nothing remaining, we're done splitting and should wire the new node up with the child of the Project
        plan.children()[0].clone()
    } else {
        // Recursively run the rule on the new child Project
        let new_project = Project::try_new(plan.children()[0].clone(), remaining)?;
        let new_child_project = LogicalPlan::Project(new_project.clone()).arced();
        let optimized_child_plan = try_optimize_project(
            &new_project,
            new_child_project.clone(),
            num_actors,
            recursive_count + 1,
        )?;
        optimized_child_plan.unwrap().clone()
    };
    println!("New child:\n{}", new_plan_child.repr_ascii(false));

    // Start building a chain of `child -> Project -> ActorPoolProject -> ActorPoolProject -> ...`
    let (stateful_stages, stateless_stages): (Vec<_>, Vec<_>) =
        next_stages.into_iter().partition(has_stateful_udf);
    let stateless_stages_names: HashSet<String> = stateless_stages
        .iter()
        .map(|e| e.name().to_string())
        .collect();

    // Build the new stateless Project: [...stateless_projections, ...all columns that came before it]
    let stateless_projection = stateless_stages
        .into_iter()
        .chain(
            new_plan_child
                .schema()
                .fields
                .iter()
                .filter_map(|(name, _)| {
                    if stateless_stages_names.contains(name) {
                        None
                    } else {
                        Some(Expr::Column(name.clone().into()).arced())
                    }
                }),
        )
        .collect();
    let new_plan = {
        LogicalPlan::Project(Project::try_new(
            new_plan_child.clone(),
            stateless_projection,
        )?)
        .arced()
    };
    println!("With new project:\n{}", new_plan.repr_ascii(false));

    // Iteratively build ActorPoolProject nodes: [StatefulUDF, ...all columns that came before it]
    let new_plan = {
        let mut child = new_plan;

        for stateful_expr in stateful_stages {
            let stateful_expr_name = stateful_expr.name().to_string();
            let stateful_projection = child
                .schema()
                .fields
                .iter()
                .filter_map(|(name, _)| {
                    if name == &stateful_expr_name {
                        None
                    } else {
                        Some(Expr::Column(name.as_str().into()).arced())
                    }
                })
                .chain(iter::once(stateful_expr))
                .collect();
            child = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
                child,
                stateful_projection,
                num_actors,
            )?)
            .arced();
        }
        child
    };
    println!(
        "With new actor pool projects:\n{}",
        new_plan.repr_ascii(false)
    );

    // One final project to select just the columns we need
    // This will help us do the necessary column pruning via projection pushdowns
    let final_selection_project = LogicalPlan::Project(Project::try_new(
        new_plan,
        projection
            .projection
            .iter()
            .map(|e| Expr::Column(e.name().into()).arced())
            .collect(),
    )?)
    .arced();

    Ok(Transformed::Yes(final_selection_project))
}

#[inline]
fn has_stateful_udf(e: &ExprRef) -> bool {
    e.exists(|e| {
        matches!(
            e.as_ref(),
            Expr::Function {
                func: FunctionExpr::Python(PythonUDF::Stateful(_)),
                ..
            }
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_resource_request::ResourceRequest;
    use daft_core::datatypes::Field;
    use daft_dsl::{
        col,
        functions::{
            python::{PythonUDF, StatefulPythonUDF},
            FunctionExpr,
        },
        Expr, ExprRef,
    };

    use crate::{
        logical_ops::{ActorPoolProject, Project},
        logical_optimization::test::assert_optimized_plan_with_rules_eq,
        test::{dummy_scan_node, dummy_scan_operator},
        LogicalPlan,
    };

    use super::SplitActorPoolProjects;

    /// Helper that creates an optimizer with the SplitExprByStatefulUDF rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(
            plan,
            expected,
            vec![Box::new(SplitActorPoolProjects {})],
        )
    }

    #[cfg(not(feature = "python"))]
    fn create_stateful_udf(inputs: Vec<ExprRef>) -> ExprRef {
        Expr::Function {
            func: FunctionExpr::Python(PythonUDF::Stateful(StatefulPythonUDF {
                name: Arc::new("foo".to_string()),
                num_expressions: inputs.len(),
                return_dtype: daft_core::DataType::Binary,
                resource_request: Some(create_resource_request()),
            })),
            inputs,
        }
        .arced()
    }

    fn create_resource_request() -> ResourceRequest {
        ResourceRequest {
            num_cpus: Some(8.),
            num_gpus: Some(1.),
            memory_bytes: None,
        }
    }

    // TODO: need to figure out how users will pass this in
    static NUM_ACTORS: usize = 1;

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_with_column_stateful_udf_happypath() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        let project_plan = scan_plan
            .with_columns(vec![stateful_project_expr.clone().alias("b")], None)?
            .build();

        // Project([col("a")]) --> ActorPoolProject([col("a"), foo(col("a")).alias("b")])
        let expected = scan_plan.select(vec![col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![col("a"), stateful_project_expr.clone().alias("b")],
            NUM_ACTORS,
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[cfg(not(feature = "python"))]
    #[test]
    fn test_multiple_with_column_parallel() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stateful_project_expr = create_stateful_udf(vec![col("a")]);

        // Add a Projection with StatefulUDF and resource request
        // NOTE: Our common-subtree elimination will build this as 2 project nodes:
        // Project([col("a").alias("a"), foo(col("a")).alias(factored_column_name)])
        //   --> Project([col("a"), col(factored_column_name).alias("b"), col(factored_column_name).alias("c")])
        let factored_column_name = "Function_Python(Stateful(StatefulPythonUDF { name: \"foo\", num_expressions: 1, return_dtype: Binary, resource_request: Some(ResourceRequest { num_cpus: Some(8.0), num_gpus: Some(1.0), memory_bytes: None }) }))(a)";
        let project_plan = scan_plan
            .with_columns(
                vec![
                    stateful_project_expr.clone().alias("b"),
                    stateful_project_expr.clone().alias("c"),
                ],
                None,
            )?
            .build();

        // Project([col("a").alias("a")])
        //   --> ActorPoolProject([col("a"), foo(col("a")).alias(factored_columns_name)])
        //   --> Project([col("a"), col(factored_columns_name).alias("b"), foo(col(factored_columns_name)).alias("c")])
        let expected = scan_plan.select(vec![col("a").alias("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                stateful_project_expr.clone().alias(factored_column_name),
            ],
            NUM_ACTORS,
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![
                col("a"),
                col(factored_column_name).alias("b"),
                col(factored_column_name).alias("c"),
            ],
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;

        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![Field::new("a", daft_core::DataType::Utf8)]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr =
            create_stateful_udf(vec![create_stateful_udf(vec![col("a")])]);

        // Add a Projection with StatefulUDF and resource request
        // Project([col("a"), foo(foo(col("a"))).alias("b")])
        let project_plan = scan_plan
            .with_columns(vec![stacked_stateful_project_expr.clone().alias("b")], None)?
            .build();

        //   Project([col("a")])
        //   --> ActorPoolProject([col("a"), foo(col("a")).alias("__SplitExprByStatefulUDF_0-0_stateful_child__")])
        //   --> Project([col("a"), col("__SplitExprByStatefulUDF_0-0_stateful_child__")])
        //   --> ActorPoolProject([col("a"), foo(col("__SplitExprByStatefulUDF_0-0_stateful_child__")).alias("b")])
        let intermediate_name = "__SplitExprByStatefulUDF_0-0_stateful_child__";
        let expected = scan_plan.select(vec![col("a")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name),
            ],
            NUM_ACTORS,
        )?)
        .arced();
        let expected = LogicalPlan::Project(Project::try_new(
            expected,
            vec![col("a"), col(intermediate_name)],
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("a"),
                create_stateful_udf(vec![col(intermediate_name)])
                    .clone()
                    .alias("b"),
            ],
            NUM_ACTORS,
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;
        Ok(())
    }

    #[test]
    fn test_multiple_with_column_serial_multiarg() -> DaftResult<()> {
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", daft_core::DataType::Utf8),
            Field::new("b", daft_core::DataType::Utf8),
        ]);
        let scan_plan = dummy_scan_node(scan_op);
        let stacked_stateful_project_expr = create_stateful_udf(vec![
            create_stateful_udf(vec![col("a")]),
            create_stateful_udf(vec![col("b")]),
        ]);

        // Add a Projection with StatefulUDF and resource request
        // Project([foo(foo(col("a")), foo(col("b"))).alias("c")])
        let project_plan = scan_plan
            .select(vec![stacked_stateful_project_expr.clone().alias("c")])?
            .build();

        //   Project([col("a"), col("b")])
        //   --> ActorPoolProject([col("b"), foo(col("a")).alias("__SplitExprByStatefulUDF_0-0_stateful_child__")])
        //   --> ActorPoolProject([col("__SplitExprByStatefulUDF_0-0_stateful_child__"), foo(col("b")).alias("__SplitExprByStatefulUDF_0-1_stateful_child__")])
        //   --> ActorPoolProject([foo(col("__SplitExprByStatefulUDF_0-0_stateful_child__"), col("__SplitExprByStatefulUDF_0-1_stateful_child__")).alias("c")])
        let intermediate_name_0 = "__SplitExprByStatefulUDF_0-0_stateful_child__";
        let intermediate_name_1 = "__SplitExprByStatefulUDF_0-1_stateful_child__";
        let expected = scan_plan.select(vec![col("a"), col("b")])?.build();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col("b"),
                create_stateful_udf(vec![col("a")])
                    .clone()
                    .alias(intermediate_name_0),
            ],
            NUM_ACTORS,
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                col(intermediate_name_0),
                create_stateful_udf(vec![col("b")])
                    .clone()
                    .alias(intermediate_name_1),
            ],
            NUM_ACTORS,
        )?)
        .arced();
        let expected = LogicalPlan::ActorPoolProject(ActorPoolProject::try_new(
            expected,
            vec![
                create_stateful_udf(vec![col(intermediate_name_0), col(intermediate_name_1)])
                    .clone()
                    .alias("c"),
            ],
            NUM_ACTORS,
        )?)
        .arced();

        assert_optimized_plan_eq(project_plan, expected)?;
        Ok(())
    }
}
