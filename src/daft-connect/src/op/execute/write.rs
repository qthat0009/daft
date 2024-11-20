use std::{collections::HashMap, future::ready};

use common_daft_config::DaftExecutionConfig;
use common_file_formats::FileFormat;
use eyre::{bail, WrapErr};
use futures::stream;
use spark_connect::{
    write_operation::{SaveMode, SaveType},
    ExecutePlanResponse, WriteOperation,
};
use tokio_util::sync::CancellationToken;
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};
use tracing::warn;

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation,
};

impl Session {
    pub async fn handle_write_command(
        &self,
        operation: WriteOperation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id,
        };

        let finished = context.finished();

        // operation: WriteOperation {
        //     input: Some(
        //         Relation {
        //         common: Some(
        //             RelationCommon {
        //             source_info: "",
        //             plan_id: Some(
        //             0,
        //             ),
        //             origin: None,
        //         },
        //         ),
        //         rel_type: Some(
        //             Range(
        //                 Range {
        //             start: Some(
        //             0,
        //             ),
        //             end: 10,
        //             step: 1,
        //             num_partitions: None,
        //         },
        //         ),
        //         ),
        //     },
        //     ),
        //     source: Some(
        //     "parquet",
        //     ),
        //     mode: Unspecified,
        //     sort_column_names: [],
        //     partitioning_columns: [],
        //     bucket_by: None,
        //     options: {},
        //     clustering_columns: [],
        //     save_type: Some(
        //         Path(
        //     "/var/folders/zy/g1zccty96bg_frmz9x0198zh0000gn/T/tmpxki7yyr0/test.parquet",
        //     ),
        //     ),
        // }

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(16);
        std::thread::spawn(move || {
            let result = (|| -> eyre::Result<()> {
                let WriteOperation {
                    input,
                    source,
                    mode,
                    sort_column_names,
                    partitioning_columns,
                    bucket_by,
                    options,
                    clustering_columns,
                    save_type,
                } = operation;

                let Some(input) = input else {
                    bail!("Input is required");
                };

                let Some(source) = source else {
                    bail!("Source is required");
                };

                let file_format = match &*source {
                    "parquet" => FileFormat::Parquet,
                    "csv" => FileFormat::Csv,
                    "json" => FileFormat::Json,
                    _ => bail!("Unsupported source: {source}; only parquet and csv are supported"),
                };

                let Ok(mode) = SaveMode::try_from(mode) else {
                    bail!("Invalid save mode: {mode}");
                };

                if !sort_column_names.is_empty() {
                    // todo(completeness): implement sort
                    warn!(
                        "Ignoring sort_column_names: {sort_column_names:?} (not yet implemented)"
                    );
                }

                if !partitioning_columns.is_empty() {
                    // todo(completeness): implement partitioning
                    warn!("Ignoring partitioning_columns: {partitioning_columns:?} (not yet implemented)");
                }

                if let Some(bucket_by) = bucket_by {
                    // todo(completeness): implement bucketing
                    warn!("Ignoring bucket_by: {bucket_by:?} (not yet implemented)");
                }

                if !options.is_empty() {
                    // todo(completeness): implement options
                    warn!("Ignoring options: {options:?} (not yet implemented)");
                }

                if !clustering_columns.is_empty() {
                    // todo(completeness): implement clustering
                    warn!(
                        "Ignoring clustering_columns: {clustering_columns:?} (not yet implemented)"
                    );
                }

                match mode {
                    SaveMode::Unspecified => {}
                    SaveMode::Append => {}
                    SaveMode::Overwrite => {}
                    SaveMode::ErrorIfExists => {}
                    SaveMode::Ignore => {}
                }

                let Some(save_type) = save_type else {
                    bail!("Save type is required");
                };

                let path = match save_type {
                    SaveType::Path(path) => path,
                    SaveType::Table(table) => {
                        let name = table.table_name;
                        bail!("Tried to write to table {name} but it is not yet implemented. Try to write to a path instead.");
                    }
                };

                let plan = translation::to_logical_plan(input)?;

                let plan = plan
                    .table_write(&path, file_format, None, None, None)
                    .wrap_err("Failed to create table write plan")?;

                let logical_plan = plan.build();
                let physical_plan = daft_local_plan::translate(&logical_plan)?;

                let cfg = DaftExecutionConfig::default();

                // "hot" flow not a "cold" flow
                let iterator = daft_local_execution::run_local(
                    &physical_plan,
                    HashMap::new(),
                    cfg.into(),
                    None,
                    CancellationToken::new(), // todo: maybe implement cancelling
                )?;

                for _ignored in iterator {}

                // this is so we make sure the operation is actually done
                // before we return
                //
                // an example where this is important is if we write to a parquet file
                // and then read immediately after, we need to wait for the write to finish

                Ok(())
            })();

            if let Err(e) = result {
                tx.blocking_send(Err(e)).unwrap();
            }
        });

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
