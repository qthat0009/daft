use std::path::PathBuf;

use super::object_io::{GetResult, ObjectSource};
use super::Result;
use async_trait::async_trait;
use bytes::Bytes;
use snafu::{ResultExt, Snafu};
use tokio::io::AsyncReadExt;
use url::ParseError;

pub struct LocalSource {}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open file {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("Unable to parse URL \"{}\"", url.to_string_lossy()))]
    InvalidUrl { url: PathBuf, source: ParseError },

    #[snafu(display("Unable to convert URL \"{}\" to local file path", path))]
    InvalidFilePath { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source } => {
                use std::io::ErrorKind::*;
                match source.kind() {
                    NotFound => super::Error::NotFound {
                        path: path,
                        source: source.into(),
                    },
                    _ => super::Error::UnableToOpenFile {
                        path: path,
                        source: source.into(),
                    },
                }
            }
            UnableToReadBytes { path, source } => super::Error::UnableToReadBytes { path, source },
            InvalidUrl { url, source } => super::Error::InvalidUrl {
                path: url.to_string_lossy().into_owned(),
                source,
            },
            _ => super::Error::Generic {
                store: "local",
                source: error.into(),
            },
        }
    }
}

impl LocalSource {
    pub async fn new() -> Self {
        LocalSource {}
    }
}

#[async_trait]
impl ObjectSource for LocalSource {
    async fn get(&self, uri: &str) -> super::Result<GetResult> {
        let path = url::Url::parse(uri).context(InvalidUrlSnafu { url: uri })?;
        let file_path = match path.to_file_path() {
            Ok(f) => Ok(f),
            Err(_err) => Err(Error::InvalidFilePath {
                path: path.to_string(),
            }),
        }?;
        Ok(GetResult::File(file_path))
    }
}

pub(crate) async fn collect_file(path: &str) -> Result<Bytes> {
    let mut file = tokio::fs::File::open(path)
        .await
        .context(UnableToOpenFileSnafu { path })?;
    let mut buf = vec![];
    let _ = file
        .read_to_end(&mut buf)
        .await
        .context(UnableToReadBytesSnafu::<String> { path: path.into() })?;
    Ok(Bytes::from(buf))
}
