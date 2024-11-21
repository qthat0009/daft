use std::io::Cursor;

use arrow2::io::{
    ipc::{
        read::{StreamMetadata, StreamReader, StreamState},
        IpcSchema,
    },
    json_integration::read::deserialize_schema,
};
use arrow_format::ipc::MetadataVersion;
use daft_logical_plan::LogicalPlanBuilder;
use eyre::{bail, WrapErr};

pub fn local_relation(plan: spark_connect::LocalRelation) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::LocalRelation { data, schema } = plan;

    let Some(data) = data else {
        return bail!("Data is required");
    };

    let Some(schema) = schema else {
        return bail!("Schema is required");
    };

    let schema: serde_json::Value = serde_json::from_str(&schema)
        .wrap_err_with(|| format!("Failed to deserialize schema: {schema}"))?;

    let (schema, ipc_fields) = deserialize_schema(&schema)?;

    let metadata = StreamMetadata {
        schema,
        version: MetadataVersion::V1,
        ipc_schema: IpcSchema {
            fields: ipc_fields,
            is_little_endian: false,
        }, // todo(corectness): unsure about endianness
    };

    let reader = Cursor::new(&data);
    let reader = StreamReader::new(reader, metadata, None); // todo(corectness): unsure about projection

    for value in reader {
        let value = value?;

        match value {
            StreamState::Waiting => {
                bail!("Somehow got a waiting state; we should never get this (we have all data upfront)");
            }
            StreamState::Some(v) => {
                todo!()
            }
        }
    }

    todo!()
}
