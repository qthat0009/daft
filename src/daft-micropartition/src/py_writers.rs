use std::sync::Arc;

use common_error::DaftResult;
use daft_table::{python::PyTable, Table};
use pyo3::{types::PyAnyMethods, PyObject, Python};

use crate::{python::PyMicroPartition, FileWriter, MicroPartition};

pub struct PyArrowParquetWriter {
    py_writer: PyObject,
}

impl PyArrowParquetWriter {
    pub fn new(
        root_dir: &str,
        file_idx: usize,
        compression: &Option<String>,
        io_config: &Option<daft_io::IOConfig>,
        partition_values: Option<&Table>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import_bound(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("ParquetFileWriter")?;

            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values.map(|pv| PyTable::from(pv.clone())),
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
}

impl FileWriter for PyArrowParquetWriter {
    fn write(&self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        Python::with_gil(|py| {
            let py_micropartition = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data.clone()),))?;
            self.py_writer
                .call_method1(py, "write", (py_micropartition,))?;
            Ok(())
        })
    }

    fn close(&self) -> DaftResult<Option<Table>> {
        Python::with_gil(|py| {
            let result = self.py_writer.call_method0(py, "close")?;
            Ok(Some(result.extract::<PyTable>(py)?.into()))
        })
    }
}

pub struct PyArrowCSVWriter {
    py_writer: PyObject,
}

impl PyArrowCSVWriter {
    pub fn new(
        root_dir: &str,
        file_idx: usize,
        io_config: &Option<daft_io::IOConfig>,
        partition_values: Option<&Table>,
    ) -> DaftResult<Self> {
        Python::with_gil(|py| {
            let file_writer_module = py.import_bound(pyo3::intern!(py, "daft.io.writer"))?;
            let file_writer_class = file_writer_module.getattr("CSVFileWriter")?;

            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                partition_values.map(|pv| PyTable::from(pv.clone())),
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

impl FileWriter for PyArrowCSVWriter {
    fn write(&self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        Python::with_gil(|py| {
            let py_micropartition = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(data.clone()),))?;
            self.py_writer
                .call_method1(py, "write", (py_micropartition,))?;
            Ok(())
        })
    }

    fn close(&self) -> DaftResult<Option<Table>> {
        Python::with_gil(|py| {
            let result = self.py_writer.call_method0(py, "close")?;
            Ok(Some(result.extract::<PyTable>(py)?.into()))
        })
    }
}
