use std::{sync::Arc, vec};

use common_error::{DaftError, DaftResult};
use daft_core::schema::SchemaRef;
use daft_csv::CsvParseOptions;
use daft_io::{parse_url, FileMetadata, IOClient, IOStatsContext, IOStatsRef};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::Snafu;

use crate::{
    file_format::{CsvSourceConfig, FileFormatConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    DataFileSource, PartitionField, Pushdowns, ScanOperator, ScanTask, ScanTaskRef,
};
#[derive(Debug)]
pub struct GlobScanOperator {
    glob_paths: Vec<String>,
    file_format_config: Arc<FileFormatConfig>,
    schema: SchemaRef,
    storage_config: Arc<StorageConfig>,
}

/// Wrapper struct that implements a sync Iterator for a BoxStream
struct BoxStreamIterator<'a, T> {
    boxstream: BoxStream<'a, T>,
    runtime_handle: tokio::runtime::Handle,
}

impl<'a, T> Iterator for BoxStreamIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime_handle
            .block_on(async { self.boxstream.next().await })
    }
}

#[derive(Snafu, Debug)]
enum Error {
    #[snafu(display(
        "Glob path had no matches: \"{}\". \nTo search for files recursively, use '{}/**'.",
        glob_path,
        glob_path.trim_end_matches('/'),
    ))]
    GlobNoMatch { glob_path: String },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        match &value {
            Error::GlobNoMatch { glob_path } => DaftError::FileNotFound {
                path: glob_path.clone(),
                source: Box::new(value),
            },
        }
    }
}

type FileInfoIterator = Box<dyn Iterator<Item = DaftResult<FileMetadata>>>;

fn run_glob(
    glob_path: &str,
    limit: Option<usize>,
    io_client: Arc<IOClient>,
    runtime: Arc<tokio::runtime::Runtime>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<FileInfoIterator> {
    let (_, parsed_glob_path) = parse_url(glob_path)?;
    // Construct a static-lifetime BoxStream returning the FileMetadata
    let glob_input = parsed_glob_path.as_ref().to_string();
    let runtime_handle = runtime.handle();
    let boxstream = runtime_handle.block_on(async move {
        io_client
            .glob(glob_input, None, None, limit, io_stats)
            .await
    })?;

    // Construct a static-lifetime BoxStreamIterator
    let iterator = BoxStreamIterator {
        boxstream,
        runtime_handle: runtime_handle.clone(),
    };
    let iterator = iterator.map(|fm| Ok(fm?));
    Ok(Box::new(iterator))
}

fn run_glob_parallel(
    glob_paths: Vec<String>,
    io_client: Arc<IOClient>,
    runtime: Arc<tokio::runtime::Runtime>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<impl Iterator<Item = DaftResult<FileMetadata>>> {
    let num_parallel_tasks = 64;

    let owned_runtime = runtime.clone();
    let boxstream = futures::stream::iter(glob_paths.into_iter().map(move |path| {
        let (_, parsed_glob_path) = parse_url(&path).unwrap();
        let glob_input = parsed_glob_path.as_ref().to_string();
        let io_client = io_client.clone();
        let io_stats = io_stats.clone();

        runtime.spawn(async move {
            let stream = io_client
                .glob(glob_input, None, None, None, io_stats)
                .await?;
            let results = stream.collect::<Vec<_>>().await;
            Result::<_, daft_io::Error>::Ok(futures::stream::iter(results))
        })
    }))
    .buffered(num_parallel_tasks)
    .map(|v| v.map_err(|e| daft_io::Error::JoinError { source: e })?)
    .try_flatten()
    .map(|v| Ok(v?))
    .boxed();

    // Construct a static-lifetime BoxStreamIterator
    let iterator = BoxStreamIterator {
        boxstream,
        runtime_handle: owned_runtime.handle().clone(),
    };
    Ok(iterator)
}

impl GlobScanOperator {
    pub fn try_new(
        glob_paths: &[&str],
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        schema_hint: Option<SchemaRef>,
        schema_override: Option<SchemaRef>,
    ) -> DaftResult<Self> {
        let first_glob_path = match glob_paths.first() {
            None => Err(DaftError::ValueError(
                "Cannot glob empty list of files".to_string(),
            )),
            Some(path) => Ok(path),
        }?;

        let (io_runtime, io_client) = storage_config.get_io_client_and_runtime()?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::try_new schema inference for {first_glob_path}"
        ));
        let mut paths = run_glob(
            first_glob_path,
            Some(1),
            io_client.clone(),
            io_runtime.clone(),
            Some(io_stats.clone()),
        )?;
        let FileMetadata {
            filepath: first_filepath,
            ..
        } = match paths.next() {
            Some(file_metadata) => file_metadata,
            None => Err(Error::GlobNoMatch {
                glob_path: first_glob_path.to_string(),
            }
            .into()),
        }?;

        println!("schema_override: {:?}", schema_override);

        let schema = match schema_override {
            None => {
                let inferred_schema = match file_format_config.as_ref() {
                    FileFormatConfig::Parquet(ParquetSourceConfig {
                        coerce_int96_timestamp_unit,
                        field_id_mapping,
                    }) => {
                        let io_stats = IOStatsContext::new(format!(
                            "GlobScanOperator constructor read_parquet_schema: for uri {first_filepath}"
                        ));
                        daft_parquet::read::read_parquet_schema(
                            first_filepath.as_str(),
                            io_client.clone(),
                            Some(io_stats),
                            ParquetSchemaInferenceOptions {
                                coerce_int96_timestamp_unit: *coerce_int96_timestamp_unit,
                            },
                            field_id_mapping.clone(),
                        )?
                    }
                    FileFormatConfig::Csv(CsvSourceConfig {
                        delimiter,
                        has_headers,
                        double_quote,
                        quote,
                        escape_char,
                        comment,
                        ..
                    }) => {
                        let (schema, _) = daft_csv::metadata::read_csv_schema(
                            first_filepath.as_str(),
                            Some(CsvParseOptions::new_with_defaults(
                                *has_headers,
                                *delimiter,
                                *double_quote,
                                *quote,
                                *escape_char,
                                *comment,
                            )?),
                            None,
                            io_client,
                            Some(io_stats),
                        )?;
                        schema
                    }
                    FileFormatConfig::Json(_) => daft_json::schema::read_json_schema(
                        first_filepath.as_str(),
                        None,
                        None,
                        io_client,
                        Some(io_stats),
                    )?,
                    #[cfg(feature = "python")]
                    FileFormatConfig::Database(_) => {
                        return Err(DaftError::ValueError(
                            "Cannot glob a database source".to_string(),
                        ))
                    }
                    #[cfg(feature = "python")]
                    FileFormatConfig::PythonFunction => {
                        return Err(DaftError::ValueError(
                            "Cannot glob a PythonFunction source".to_string(),
                        ))
                    }
                };
                match schema_hint {
                    Some(hint) => Arc::new(inferred_schema.apply_hints(&hint)?),
                    None => Arc::new(inferred_schema),
                }
            }
            Some(schema_override) => schema_override,
        };

        Ok(Self {
            glob_paths: glob_paths.iter().map(|s| s.to_string()).collect(),
            file_format_config,
            schema,
            storage_config,
        })
    }
}

impl ScanOperator for GlobScanOperator {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
    }

    fn can_absorb_filter(&self) -> bool {
        false
    }
    fn can_absorb_select(&self) -> bool {
        false
    }
    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![
            "GlobScanOperator".to_string(),
            format!("Glob paths = [{}]", self.glob_paths.join(", ")),
        ];
        lines.extend(self.file_format_config.multiline_display());
        lines.extend(self.storage_config.multiline_display());

        lines
    }

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'static>> {
        let (io_runtime, io_client) = self.storage_config.get_io_client_and_runtime()?;
        let io_stats = IOStatsContext::new(format!(
            "GlobScanOperator::to_scan_tasks for {:#?}",
            self.glob_paths
        ));

        let files = run_glob_parallel(
            self.glob_paths.clone(),
            io_client.clone(),
            io_runtime.clone(),
            Some(io_stats.clone()),
        )?;

        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        // Create one ScanTask per file
        Ok(Box::new(files.map(move |f| {
            let FileMetadata {
                filepath: path,
                size: size_bytes,
                ..
            } = f?;
            Ok(ScanTask::new(
                vec![DataFileSource::AnonymousDataFile {
                    path: path.to_string(),
                    chunk_spec: None,
                    size_bytes,
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                }],
                file_format_config.clone(),
                schema.clone(),
                storage_config.clone(),
                pushdowns.clone(),
            )
            .into())
        })))
    }
}
