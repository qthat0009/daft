use std::{collections::HashMap, future::ready};

use common_daft_config::DaftExecutionConfig;
use daft_core::series::Series;
use daft_local_execution::NativeExecutor;
use daft_schema::{dtype::DataType, field::Field, schema::Schema};
use daft_table::Table;
use futures::stream;
use spark_connect::{ExecutePlanResponse, Relation};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation,
};

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id,
        };

        let finished = context.finished();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);
        tokio::spawn(async move {
            let execution_fut = async {
                let plan = translation::to_logical_plan(command)?;
                let optimized_plan = plan.optimize()?;
                let cfg = DaftExecutionConfig::default();
                let native_executor = NativeExecutor::from_logical_plan_builder(&optimized_plan)?;
                let mut result_stream = native_executor
                    .run(HashMap::new(), cfg.into(), None)?
                    .into_stream();
                while let Some(result) = result_stream.next().await {
                    let result = result?;
                    let tables = result.get_tables()?;

                    for table in tables.as_slice() {
                        // Inside the for loop over tables
                        let mut arrow_arrays = Vec::with_capacity(table.num_columns());
                        let mut column_names = Vec::with_capacity(table.num_columns());
                        let mut field_types = Vec::with_capacity(table.num_columns());

                        for i in 0..table.num_columns() {
                            let s = table.get_column_by_index(i)?;
                            let arrow_array = s.to_arrow();

                            let arrow_array =
                                daft_core::utils::arrow::cast_array_from_daft_if_needed(
                                    arrow_array.to_boxed(),
                                );

                            // todo(correctness): logical types probably get **DESTROYED** here 💥😭😭
                            let daft_data_type = DataType::from(arrow_array.data_type());

                            // Store the actual type after potential casting
                            field_types.push(Field::new(s.name(), daft_data_type));
                            column_names.push(s.name().to_string());
                            arrow_arrays.push(arrow_array);
                        }

                        // Create new schema with actual types after casting
                        let new_schema = Schema::new(field_types)?;

                        println!("new schema: {:?}", new_schema);

                        // Convert arrays back to series
                        let series = arrow_arrays
                            .into_iter()
                            .zip(column_names)
                            .map(|(array, name)| Series::try_from((name.as_str(), array)))
                            .try_collect()?;

                        // Create table from series
                        let new_table = Table::new_with_size(new_schema, series, table.len())?;

                        let response = context.gen_response(&new_table)?;
                        tx.send(Ok(response)).await.unwrap();
                    }
                }

                Ok(())
            };

            if let Err(e) = execution_fut.await {
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")))
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }
}
