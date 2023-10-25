

pub mod pylib {
    use std::str::FromStr;
    use std::sync::Arc;

    use daft_core::python::schema::PySchema;
    use daft_core::schema::Schema;
    use pyo3::prelude::*;
    use pyo3::pyclass;

    use crate::FileType;
    use crate::ScanOperatorRef;
    use crate::anonymous::AnonymousScanOperator;
    
    #[pyclass(module = "daft.daft", frozen)]
    struct ScanOperator {
        scan_op: ScanOperatorRef
    }

    #[pymethods]
    impl ScanOperator {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.scan_op))
        }


        #[staticmethod]
        pub fn anonymous_scan(schema: PySchema, file_type: &str, files: Vec<String>) -> PyResult<Self> {
            let schema = schema.schema;
            let operator = Box::new(AnonymousScanOperator::new(schema, FileType::from_str(file_type)?, files));
            Ok(ScanOperator {
                scan_op: operator
            })
        }
    }

}