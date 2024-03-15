use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
};

use crate::{
    sink_info::{CatalogType, SinkInfo},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sink {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub schema: SchemaRef,
    /// Information about the sink data location.
    pub sink_info: Arc<SinkInfo>,
}

impl Sink {
    pub(crate) fn try_new(input: Arc<LogicalPlan>, sink_info: Arc<SinkInfo>) -> DaftResult<Self> {
        let schema = input.schema();

        let fields = match sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                let mut fields = vec![Field::new("path", daft_core::DataType::Utf8)];
                if let Some(ref pcols) = output_file_info.partition_cols {
                    for pc in pcols {
                        fields.push(pc.to_field(&schema)?);
                    }
                }
                fields
            }
            SinkInfo::CatalogInfo(..) => {
                vec![
                    Field::new("operation", daft_core::DataType::Utf8),
                    Field::new("path", daft_core::DataType::Utf8),
                ]
            }
        };
        let schema = Schema::new(fields)?.into();
        Ok(Self {
            input,
            schema,
            sink_info,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.sink_info.as_ref() {
            SinkInfo::OutputFileInfo(output_file_info) => {
                res.push(format!("Sink: {:?}", output_file_info.file_format));
                res.extend(output_file_info.multiline_display());
            }
            SinkInfo::CatalogInfo(catalog_info) => {
                match &catalog_info.catalog {
                    CatalogType::Iceberg(iceberg_info) => {
                        res.push(format!("Sink: Iceberg({})", iceberg_info.table_name));
                        // TODO multiline display for iceberg
                    }
                }
            }
        }
        res.push(format!("Output schema = {}", self.schema.short_string()));
        res
    }
}
