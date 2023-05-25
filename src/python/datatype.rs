use std::str::FromStr;

use crate::datatypes::{DataType, Field, ImageMode};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict, PyString, PyTuple},
};

#[pyclass]
#[derive(Clone)]
pub struct PyDataType {
    pub dtype: DataType,
}

#[pymethods]
impl PyDataType {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            0 => Ok(DataType::new_null().into()),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyDataType, got : {}",
                args.len()
            ))),
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.dtype))
    }

    #[staticmethod]
    pub fn null() -> PyResult<Self> {
        Ok(DataType::Null.into())
    }

    #[staticmethod]
    pub fn bool() -> PyResult<Self> {
        Ok(DataType::Boolean.into())
    }

    #[staticmethod]
    pub fn int8() -> PyResult<Self> {
        Ok(DataType::Int8.into())
    }

    #[staticmethod]
    pub fn int16() -> PyResult<Self> {
        Ok(DataType::Int16.into())
    }

    #[staticmethod]
    pub fn int32() -> PyResult<Self> {
        Ok(DataType::Int32.into())
    }

    #[staticmethod]
    pub fn int64() -> PyResult<Self> {
        Ok(DataType::Int64.into())
    }

    #[staticmethod]
    pub fn uint8() -> PyResult<Self> {
        Ok(DataType::UInt8.into())
    }

    #[staticmethod]
    pub fn uint16() -> PyResult<Self> {
        Ok(DataType::UInt16.into())
    }

    #[staticmethod]
    pub fn uint32() -> PyResult<Self> {
        Ok(DataType::UInt32.into())
    }

    #[staticmethod]
    pub fn uint64() -> PyResult<Self> {
        Ok(DataType::UInt64.into())
    }

    #[staticmethod]
    pub fn float32() -> PyResult<Self> {
        Ok(DataType::Float32.into())
    }

    #[staticmethod]
    pub fn float64() -> PyResult<Self> {
        Ok(DataType::Float64.into())
    }

    #[staticmethod]
    pub fn binary() -> PyResult<Self> {
        Ok(DataType::Binary.into())
    }

    #[staticmethod]
    pub fn string() -> PyResult<Self> {
        Ok(DataType::Utf8.into())
    }

    #[staticmethod]
    pub fn date() -> PyResult<Self> {
        Ok(DataType::Date.into())
    }

    #[staticmethod]
    pub fn list(name: &str, data_type: Self) -> PyResult<Self> {
        Ok(DataType::List(Box::new(Field::new(name, data_type.dtype))).into())
    }

    #[staticmethod]
    pub fn fixed_size_list(name: &str, data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for fixed-size list types must be a positive integer, but got: {}",
                size
            )));
        }
        Ok(DataType::FixedSizeList(
            Box::new(Field::new(name, data_type.dtype)),
            usize::try_from(size)?,
        )
        .into())
    }

    #[staticmethod]
    pub fn r#struct(fields: &PyDict) -> PyResult<Self> {
        Ok(DataType::Struct(
            fields
                .iter()
                .map(|(name, dtype)| {
                    Ok(Field::new(
                        name.downcast::<PyString>()?.to_str()?,
                        dtype.extract::<PyDataType>()?.dtype,
                    ))
                })
                .collect::<PyResult<Vec<Field>>>()?,
        )
        .into())
    }

    #[staticmethod]
    pub fn extension(
        name: &str,
        storage_data_type: Self,
        metadata: Option<&str>,
    ) -> PyResult<Self> {
        Ok(DataType::Extension(
            name.to_string(),
            Box::new(storage_data_type.dtype),
            metadata.map(|s| s.to_string()),
        )
        .into())
    }

    #[staticmethod]
    pub fn embedding(name: &str, data_type: Self, size: i64) -> PyResult<Self> {
        if size <= 0 {
            return Err(PyValueError::new_err(format!(
                "The size for embedding types must be a positive integer, but got: {}",
                size
            )));
        }
        if !data_type.dtype.is_numeric() {
            return Err(PyValueError::new_err(format!(
                "The data type for an embedding must be numeric, but got: {}",
                data_type.dtype
            )));
        }

        Ok(DataType::Embedding(
            Box::new(Field::new(name, data_type.dtype)),
            usize::try_from(size)?,
        )
        .into())
    }

    #[staticmethod]
    pub fn image(mode: Option<&PyAny>, size: Option<Vec<usize>>) -> PyResult<Self> {
        let image_mode = mode.and_then(|m| {
            let mode_str = m
                .getattr("value")
                .ok()?
                .downcast::<PyString>()
                .ok()?
                .to_str()
                .ok()?;
            Some(Box::new(ImageMode::from_str(mode_str).ok()?))
        });
        // TODO(Clark): Make dtype optional instead of falling back to UInt8 here.
        let dtype = image_mode.clone().map_or(Box::new(DataType::UInt8), |m| {
            Box::new(DataType::from(m.as_ref()))
        });
        if !dtype.is_numeric() {
            panic!(
                "The data type for an image must be numeric, but got: {}",
                dtype
            );
        }
        match size {
            Some(size) => {
                if size.is_empty() {
                    return Err(PyValueError::new_err(format!(
                        "The size for fixed-shape image types must be non-empty, but got: {:?}",
                        size,
                    )));
                }
                let image_mode = image_mode.ok_or(PyValueError::new_err(
                    "Image mode must be provided if specifying an image size.",
                ))?;
                Ok(DataType::FixedShapeImage(dtype, image_mode, size).into())
            }
            None => Ok(DataType::Image(dtype, image_mode).into()),
        }
    }

    #[staticmethod]
    pub fn python() -> PyResult<Self> {
        Ok(DataType::Python.into())
    }

    pub fn is_equal(&self, other: &PyAny) -> PyResult<bool> {
        if other.is_instance_of::<PyDataType>()? {
            let other = other.extract::<PyDataType>()?;
            Ok(self.dtype == other.dtype)
        } else {
            Ok(false)
        }
    }

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyBytes>(py) {
            Ok(s) => {
                self.dtype = bincode::deserialize(s.as_bytes()).unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &bincode::serialize(&self.dtype).unwrap()).to_object(py))
    }

    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.dtype.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        PyDataType { dtype: value }
    }
}

impl From<PyDataType> for DataType {
    fn from(item: PyDataType) -> Self {
        item.dtype
    }
}
