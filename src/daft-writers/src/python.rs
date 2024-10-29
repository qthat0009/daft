use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_micropartition::{python::PyMicroPartition, MicroPartition};
use daft_table::{python::PyTable, Table};
use pyo3::{types::PyAnyMethods, PyObject, Python};

use crate::FileWriter;

pub fn create_pyarrow_file_writer(
    root_dir: &str,
    file_idx: usize,
    compression: &Option<String>,
    io_config: &Option<daft_io::IOConfig>,
    format: FileFormat,
    partition: Option<&Table>,
) -> DaftResult<Box<dyn FileWriter<Input = Arc<MicroPartition>, Result = Option<Table>>>> {
    match format {
        #[cfg(feature = "python")]
        FileFormat::Parquet => Ok(Box::new(PyArrowWriter::new_parquet_writer(
            root_dir,
            file_idx,
            compression,
            io_config,
            partition,
        )?)),
        #[cfg(feature = "python")]
        FileFormat::Csv => Ok(Box::new(PyArrowWriter::new_csv_writer(
            root_dir, file_idx, io_config, partition,
        )?)),
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for physical write".to_string(),
        )),
    }
}

pub struct PyArrowWriter {
    py_writer: PyObject,
}

impl PyArrowWriter {
    pub fn new_parquet_writer(
        root_dir: &str,
        file_idx: usize,
        compression: &Option<String>,
        io_config: &Option<daft_io::IOConfig>,
        partition_values: Option<&Table>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import_bound(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("ParquetFileWriter")?;
            let _from_pytable = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "Table"))?
                .getattr(pyo3::intern!(py, "_from_pytable"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_table = _from_pytable.call1((PyTable::from(pv.clone()),))?;
                    Some(py_table)
                }
                None => None,
            };

            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values,
                compression.as_ref().map(|c| c.as_str()),
                io_config.as_ref().map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
            })
        })
    }

    pub fn new_csv_writer(
        root_dir: &str,
        file_idx: usize,
        io_config: &Option<daft_io::IOConfig>,
        partition_values: Option<&Table>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import_bound(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("CSVFileWriter")?;
            let _from_pytable = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "Table"))?
                .getattr(pyo3::intern!(py, "_from_pytable"))?;
            let partition_values = match partition_values {
                Some(pv) => {
                    let py_table = _from_pytable.call1((PyTable::from(pv.clone()),))?;
                    Some(py_table)
                }
                None => None,
            };
            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values,
                io_config.as_ref().map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
            ))?;
            Ok(Self {
                py_writer: py_writer.into(),
            })
        })
    }
}

impl FileWriter for PyArrowWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, data: &Self::Input) -> DaftResult<()> {
        Python::with_gil(|py| {
            let py_micropartition = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data.clone()),))?;
            self.py_writer
                .call_method1(py, pyo3::intern!(py, "write"), (py_micropartition,))?;
            Ok(())
        })
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        Python::with_gil(|py| {
            let result = self
                .py_writer
                .call_method0(py, pyo3::intern!(py, "close"))?
                .getattr(py, pyo3::intern!(py, "_table"))?;
            Ok(Some(result.extract::<PyTable>(py)?.into()))
        })
    }
}
