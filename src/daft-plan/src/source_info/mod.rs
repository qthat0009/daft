pub mod file_info;
use daft_core::schema::SchemaRef;
use daft_scan::ScanExternalInfo;
pub use file_info::{FileInfo, FileInfos};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;

use crate::{partitioning::ClusteringSpecRef, ClusteringSpec};

#[cfg(feature = "python")]
use {
    daft_scan::py_object_serde::{deserialize_py_object, serialize_py_object},
    pyo3::{PyObject, Python},
    std::hash::Hasher,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SourceInfo {
    InMemoryInfo(InMemoryInfo),
    ExternalInfo(ScanExternalInfo),
    PlaceHolderInfo(PlaceHolderInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryInfo {
    pub source_schema: SchemaRef,
    pub cache_key: String,
    #[cfg(feature = "python")]
    #[serde(
        serialize_with = "serialize_py_object",
        deserialize_with = "deserialize_py_object"
    )]
    pub cache_entry: PyObject,
    pub num_partitions: usize,
    pub size_bytes: usize,
    pub clustering_spec: Option<ClusteringSpecRef>,
}

#[cfg(feature = "python")]
impl InMemoryInfo {
    pub fn new(
        source_schema: SchemaRef,
        cache_key: String,
        cache_entry: PyObject,
        num_partitions: usize,
        size_bytes: usize,
        clustering_spec: Option<ClusteringSpecRef>,
    ) -> Self {
        Self {
            source_schema,
            cache_key,
            cache_entry,
            num_partitions,
            size_bytes,
            clustering_spec,
        }
    }
}

impl PartialEq for InMemoryInfo {
    fn eq(&self, other: &Self) -> bool {
        self.cache_key == other.cache_key
    }
}

impl Eq for InMemoryInfo {}

impl Hash for InMemoryInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cache_key.hash(state);
    }
}

static PLACEHOLDER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PlaceHolderInfo {
    pub source_schema: SchemaRef,
    pub clustering_spec: ClusteringSpecRef,
    pub source_id: usize,
}

impl PlaceHolderInfo {
    pub fn new(source_schema: SchemaRef, clustering_spec: ClusteringSpecRef) -> Self {
        PlaceHolderInfo {
            source_schema,
            clustering_spec,
            source_id: PLACEHOLDER_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        }
    }
}
