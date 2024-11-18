use daft_logical_plan::LogicalPlanBuilder;
use daft_scan::python::pylib::ScanOperatorHandle;
use eyre::{bail, ensure, Context};
use pyo3::prelude::*;
use spark_connect::{relation::RelType, Range, Relation};
use tracing::warn;

pub fn to_logical_plan(relation: Relation) -> eyre::Result<LogicalPlanBuilder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

fn range(range: Range) -> eyre::Result<LogicalPlanBuilder> {
    let Range {
        start,
        end,
        step,
        num_partitions,
    } = range;

    if let Some(partitions) = num_partitions {
        warn!("{partitions} ignored");
    }

    let start = start.unwrap_or(0);

    let step = usize::try_from(step).wrap_err("step must be a positive integer")?;
    ensure!(step > 0, "step must be greater than 0");

    let plan = Python::with_gil(|py| {
        let range_module = PyModule::import_bound(py, "daft.io.range")
            .wrap_err("Failed to import daft.io.range")?;

        let range = range_module
            .getattr(pyo3::intern!(py, "RangeScanOperator"))
            .wrap_err("Failed to get range function")?;

        let range = range
            .call1((start, end, step))
            .wrap_err("Failed to create range scan operator")?
            .to_object(py);

        let scan_operator_handle = ScanOperatorHandle::from_python_scan_operator(range, py)?;

        let plan = LogicalPlanBuilder::table_scan(scan_operator_handle.into(), None)?;

        eyre::Result::<_>::Ok(plan)
    })
    .wrap_err("Failed to create range scan")?;

    Ok(plan)
}