use pyo3::prelude::*;

pub mod pylib {
    use std::sync::Arc;

    use pyo3::prelude::*;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;
    use serde::{Deserialize, Serialize};

    use crate::anonymous::AnonymousScanOperator;
    use crate::file_format::PyFileFormatConfig;
    use crate::glob::GlobScanOperator;
    use crate::storage_config::PyStorageConfig;
    use crate::{ScanOperatorRef, ScanTaskBatch};

    #[pyclass(module = "daft.daft", frozen)]
    #[derive(Debug, Clone)]
    pub struct ScanOperatorHandle {
        scan_op: ScanOperatorRef,
    }

    #[pymethods]
    impl ScanOperatorHandle {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.scan_op))
        }

        #[staticmethod]
        pub fn anonymous_scan(
            files: Vec<String>,
            schema: PySchema,
            file_format_config: PyFileFormatConfig,
            storage_config: PyStorageConfig,
        ) -> PyResult<Self> {
            let schema = schema.schema;
            let operator = Arc::new(AnonymousScanOperator::new(
                files,
                schema,
                file_format_config.into(),
                storage_config.into(),
            ));
            Ok(ScanOperatorHandle {
                scan_op: ScanOperatorRef(operator),
            })
        }

        #[staticmethod]
        pub fn glob_scan(
            glob_path: &str,
            file_format_config: PyFileFormatConfig,
            storage_config: PyStorageConfig,
            schema: Option<PySchema>,
        ) -> PyResult<Self> {
            let operator = Arc::new(GlobScanOperator::try_new(
                glob_path,
                file_format_config.into(),
                storage_config.into(),
                schema.map(|s| s.schema),
            )?);
            Ok(ScanOperatorHandle {
                scan_op: ScanOperatorRef(operator),
            })
        }
    }

    impl From<ScanOperatorRef> for ScanOperatorHandle {
        fn from(value: ScanOperatorRef) -> Self {
            Self { scan_op: value }
        }
    }

    impl From<ScanOperatorHandle> for ScanOperatorRef {
        fn from(value: ScanOperatorHandle) -> Self {
            value.scan_op
        }
    }

    #[pyclass(module = "daft.daft", name = "ScanTaskBatch", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyScanTaskBatch(pub Arc<ScanTaskBatch>);

    #[pymethods]
    impl PyScanTaskBatch {
        pub fn __len__(&self) -> PyResult<usize> {
            Ok(self.0.sources.len())
        }

        pub fn slice(&self, start: usize, end: usize) -> PyResult<PyScanTaskBatch> {
            Ok(PyScanTaskBatch(Arc::new(self.0.slice(start, end))))
        }

        pub fn num_rows(&self) -> PyResult<Option<i64>> {
            Ok(self.0.num_rows().map(i64::try_from).transpose()?)
        }

        pub fn size_bytes(&self) -> PyResult<Option<i64>> {
            Ok(self.0.size_bytes().map(i64::try_from).transpose()?)
        }
    }

    impl From<Arc<ScanTaskBatch>> for PyScanTaskBatch {
        fn from(value: Arc<ScanTaskBatch>) -> Self {
            Self(value)
        }
    }

    impl From<PyScanTaskBatch> for Arc<ScanTaskBatch> {
        fn from(value: PyScanTaskBatch) -> Self {
            value.0
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    parent.add_class::<pylib::PyScanTaskBatch>()?;
    Ok(())
}
