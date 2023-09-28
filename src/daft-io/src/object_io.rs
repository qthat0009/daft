use std::ops::Range;
use std::sync::Arc;

use async_recursion::async_recursion;
use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use globset::GlobBuilder;
use tokio::sync::mpsc::Sender;
use tokio::sync::OwnedSemaphorePermit;

use crate::local::{collect_file, LocalFile};

pub enum GetResult {
    File(LocalFile),
    Stream(
        BoxStream<'static, super::Result<Bytes>>,
        Option<usize>,
        Option<OwnedSemaphorePermit>,
    ),
}

async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> super::Result<Bytes>
where
    S: Stream<Item = super::Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()?.unwrap_or_default();
    // Avoid copying if single response
    match stream.next().await.transpose()? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}

impl GetResult {
    pub async fn bytes(self) -> super::Result<Bytes> {
        use GetResult::*;
        match self {
            File(f) => collect_file(f).await,
            Stream(stream, size, _permit) => collect_bytes(stream, size).await,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    File,
    Directory,
}

impl TryFrom<std::fs::FileType> for FileType {
    type Error = DaftError;

    fn try_from(value: std::fs::FileType) -> Result<Self, Self::Error> {
        if value.is_dir() {
            Ok(Self::Directory)
        } else if value.is_file() {
            Ok(Self::File)
        } else if value.is_symlink() {
            Err(DaftError::InternalError(format!("Symlinks should never be encountered when constructing FileMetadata, but got: {:?}", value)))
        } else {
            unreachable!(
                "Can only be a directory, file, or symlink, but got: {:?}",
                value
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileMetadata {
    pub filepath: String,
    pub size: Option<u64>,
    pub filetype: FileType,
}
#[derive(Debug)]
pub struct LSResult {
    pub files: Vec<FileMetadata>,
    pub continuation_token: Option<String>,
}

use async_stream::stream;

#[async_trait]
pub(crate) trait ObjectSource: Sync + Send {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult>;
    async fn get_range(&self, uri: &str, range: Range<usize>) -> super::Result<GetResult> {
        self.get(uri, Some(range)).await
    }
    async fn get_size(&self, uri: &str) -> super::Result<usize>;

    async fn ls(
        &self,
        path: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> super::Result<LSResult>;

    async fn iter_dir(
        &self,
        uri: &str,
        delimiter: Option<&str>,
        _limit: Option<usize>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let delimiter = delimiter.map(String::from);
        let s = stream! {
            let lsr = self.ls(&uri, delimiter.as_deref(), None).await?;
            let mut continuation_token = lsr.continuation_token.clone();
            for file in lsr.files {
                yield Ok(file);
            }

            while continuation_token.is_some() {
                let lsr = self.ls(&uri, delimiter.as_deref(), continuation_token.as_deref()).await?;
                continuation_token = lsr.continuation_token.clone();
                for file in lsr.files {
                    yield Ok(file);
                }
            }
        };
        Ok(s.boxed())
    }
}

pub(crate) async fn glob(
    source: Arc<dyn ObjectSource>,
    glob: &str,
) -> super::Result<Vec<FileMetadata>> {
    // TODO: we should derive this depending on backend, will probably fail on Windows + local fs
    let delimiter = "/";

    // Parse glob fragments
    let mut glob_fragments = glob.split(delimiter).fold(
        (vec![], String::new()),
        |(mut acc, fragment_so_far), current_fragment| {
            if current_fragment.contains('*') {
                acc.push(fragment_so_far);
                acc.push(current_fragment.to_string());
                (acc, String::new())
            } else {
                (
                    acc,
                    format!("{fragment_so_far}{delimiter}{current_fragment}"),
                )
            }
        },
    );
    let glob_fragments = if glob_fragments.1.is_empty() {
        glob_fragments.0
    } else {
        glob_fragments
            .0
            .drain(..)
            .chain(std::iter::once(glob_fragments.1))
            .collect()
    };

    // Visits the specified path + next_fragment, enqueuing work and returning results where appropriate
    #[async_recursion]
    async fn visit(
        source: Arc<dyn ObjectSource>,
        path: &str,
        glob_fragments: (Vec<String>, usize),
    ) -> super::Result<Vec<FileMetadata>> {
        let (glob_fragments, i) = glob_fragments;
        let current_fragment = glob_fragments[i].as_str();

        // BASE CASE: current_fragment contains a **
        // We have no choice but to perform a naive recursive ls and filter on the results
        if current_fragment.contains("**") {
            // Perform recursive listing of everything from here on out and run it against the (full) glob filter"
            let mut all_file_metadata = recursive_iter(source.clone(), path).await?;
            let mut results = vec![];
            let glob_matcher = GlobBuilder::new(glob_fragments.join("/").as_str())
                .literal_separator(true)
                .build()
                .expect("Cannot parse glob")
                .compile_matcher();
            while let Some(fm) = all_file_metadata.next().await {
                match fm {
                    Ok(fm) => {
                        if glob_matcher.is_match(fm.filepath.as_str()) {
                            results.push(fm)
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(results)
        } else if current_fragment.contains('*') {
            // Perform a directory listing of the next level and run it against the (partial) glob filter. Then enqueue more work where necessary
            let mut next_level_file_metadata = source.iter_dir(path, Some("/"), None).await?;
            let mut matches = vec![];
            let glob_matcher = GlobBuilder::new(glob_fragments[..i + 1].join("/").as_str())
                .literal_separator(true)
                .build()
                .expect("Cannot parse glob")
                .compile_matcher();
            while let Some(fm) = next_level_file_metadata.next().await {
                match fm {
                    Ok(fm) => {
                        if glob_matcher.is_match(fm.filepath.as_str()) {
                            matches.push(fm)
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            // BASE CASE: we've reached the last remaining glob fragment and these matches are the final matches
            if i == glob_fragments.len() - 1 {
                return Ok(matches);
            }

            // RECURSIVE CASE: keep visiting recursively
            let mut results = vec![];
            for m in matches {
                let mut partial_results = visit(
                    source.clone(),
                    m.filepath.as_str(),
                    (glob_fragments.clone(), i + 1),
                )
                .await?;
                results.append(&mut partial_results);
            }
            Ok(results)
        } else {
            // Perform a directory listing of the new concatted path and enqueue that
            let full_dir_path = path.to_string() + current_fragment;

            // BASE CASE: we've reached the last remaining glob fragment and this match is the final match.
            // We need to verify that it exists before returning.
            if i == glob_fragments.len() - 1 {
                // TODO: can we implement a .exists instead? This might have weird behavior with directories
                let single_file_ls = source.ls(full_dir_path.as_str(), Some("/"), None).await?;
                if single_file_ls.files.len() == 1 {
                    return Ok(single_file_ls.files);
                } else {
                    return Ok(vec![]);
                }
            }

            // RECURSIVE CASE: keep going with the next fragment
            visit(
                source.clone(),
                full_dir_path.as_str(),
                (glob_fragments.clone(), i + 1),
            )
            .await
        }
    }

    visit(source.clone(), "", (glob_fragments, 0)).await
}

pub(crate) async fn recursive_iter(
    source: Arc<dyn ObjectSource>,
    uri: &str,
) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
    log::debug!(target: "recursive_iter", "starting recursive_iter: with top level of: {uri}");
    let (to_rtn_tx, mut to_rtn_rx) = tokio::sync::mpsc::channel(16 * 1024);
    fn add_to_channel(
        source: Arc<dyn ObjectSource>,
        tx: Sender<super::Result<FileMetadata>>,
        dir: String,
    ) {
        log::debug!(target: "recursive_iter", "recursive_iter: spawning task to list: {dir}");
        let source = source.clone();
        tokio::spawn(async move {
            let s = source.iter_dir(&dir, None, None).await;
            log::debug!(target: "recursive_iter", "started listing task for {dir}");
            let mut s = match s {
                Ok(s) => s,
                Err(e) => {
                    log::debug!(target: "recursive_iter", "Error occurred when listing {dir}\nerror:\n{e}");
                    tx.send(Err(e)).await.map_err(|se| {
                        super::Error::UnableToSendDataOverChannel { source: se.into() }
                    })?;
                    return super::Result::<_, super::Error>::Ok(());
                }
            };
            let tx = &tx;
            while let Some(tr) = s.next().await {
                if let Ok(ref tr) = tr && matches!(tr.filetype, FileType::Directory) {
                    add_to_channel(source.clone(), tx.clone(), tr.filepath.clone())
                }
                tx.send(tr)
                    .await
                    .map_err(|e| super::Error::UnableToSendDataOverChannel { source: e.into() })?;
            }
            log::debug!(target: "recursive_iter", "completed listing task for {dir}");
            super::Result::Ok(())
        });
    }

    add_to_channel(source, to_rtn_tx, uri.to_string());

    let to_rtn_stream = stream! {
        while let Some(v) = to_rtn_rx.recv().await {
            yield v
        }
    };

    Ok(to_rtn_stream.boxed())
}
