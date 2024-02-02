use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_scan::Pushdowns;

use crate::{
    physical_plan::PhysicalPlan, sink_info::OutputFileInfo, source_info::LegacyExternalInfo,
    PartitionSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TabularScanCsv {
    pub projection_schema: SchemaRef,
    pub external_info: LegacyExternalInfo,
    pub partition_spec: Arc<PartitionSpec>,
    pub pushdowns: Pushdowns,
}

impl TabularScanCsv {
    pub(crate) fn new(
        projection_schema: SchemaRef,
        external_info: LegacyExternalInfo,
        partition_spec: Arc<PartitionSpec>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            projection_schema,
            external_info,
            partition_spec,
            pushdowns,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("TabularScanCsv:".to_string());
        res.push(format!(
            "Projection schema = {}",
            self.projection_schema.short_string()
        ));
        res.extend(self.external_info.multiline_display());
        res.push(format!(
            "Partition spec = {{ {} }}",
            self.partition_spec.multiline_display().join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabularWriteCsv {
    pub schema: SchemaRef,
    pub file_info: OutputFileInfo,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl TabularWriteCsv {
    pub(crate) fn new(
        schema: SchemaRef,
        file_info: OutputFileInfo,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            schema,
            file_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("TabularWriteCsv:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.extend(self.file_info.multiline_display());
        res
    }
}
