//! Tiered persistence backend backed by Apache Arrow `object_store`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use feldera_types::config::{ObjectStorageConfig, StorageBackendConfig, StorageConfig};
use ::object_store::memory::InMemory;
use ::object_store::{DynObjectStore, ObjectStore, ObjectStoreExt, PutPayload};

use crate::block::BlockLocation;
use crate::error::StorageError;
use crate::fbuf::FBuf;
use crate::file::FileId;
use crate::tokio::TOKIO;
use crate::{
    DirEntry, FileCommitter, FileReader, FileRw, FileWriter, StorageBackend, StorageBackendFactory,
    StorageFileType, StoragePath, default_read_async,
};

/// An object storage file reader/committer.
pub struct ObjectReader {
    file_id: FileId,
    path: StoragePath,
    backend: ObjectStorageBackend,
    content: Mutex<Option<Arc<FBuf>>>,
    marked_for_checkpoint: AtomicBool,
    is_committed: AtomicBool,
    size: u64,
}

impl Debug for ObjectReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectReader")
            .field("file_id", &self.file_id)
            .field("path", &self.path)
            .field("size", &self.size)
            .field(
                "marked_for_checkpoint",
                &self.marked_for_checkpoint.load(Ordering::Relaxed),
            )
            .field("is_committed", &self.is_committed.load(Ordering::Relaxed))
            .finish()
    }
}

impl FileRw for ObjectReader {
    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn path(&self) -> &StoragePath {
        &self.path
    }
}

impl FileCommitter for ObjectReader {
    fn commit(&self) -> Result<(), StorageError> {
        if self.is_committed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let content = {
            let guard = self.content.lock().unwrap();
            guard.clone()
        };

        if let Some(buf) = content {
            let bytes = Bytes::copy_from_slice(buf.as_slice());
            let store = self.backend.store.clone();
            let path = self.path.clone();

            TOKIO.block_on(async move {
                store
                    .put(&path, PutPayload::from(bytes))
                    .await
                    .map_err(StorageError::from)
            })?;

            self.backend
                .usage
                .fetch_add(self.size as i64, Ordering::Relaxed);
        }

        Ok(())
    }
}

impl FileReader for ObjectReader {
    fn mark_for_checkpoint(&self) {
        self.marked_for_checkpoint.store(true, Ordering::SeqCst);
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        let offset = location.offset as usize;
        let size = location.size as usize;

        if (offset + size) as u64 > self.size {
            return Err(StorageError::stdio(
                ErrorKind::UnexpectedEof,
                "read past EOF",
                self.path.to_string(),
            ));
        }

        // Check if we have the content in memory.
        {
            let guard = self.content.lock().unwrap();
            if let Some(buf) = &*guard {
                let slice = &buf.as_slice()[offset..offset + size];
                return Ok(Arc::new(FBuf::from_slice(slice)));
            }
        }

        // Fetch range from underlying object store.
        let store = self.backend.store.clone();
        let path = self.path.clone();
        let range = (offset as u64)..((offset + size) as u64);

        let bytes = TOKIO.block_on(async move {
            store
                .get_range(&path, range)
                .await
                .map_err(StorageError::from)
        })?;

        let fbuf = FBuf::from_slice(&bytes);
        Ok(Arc::new(fbuf))
    }

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        default_read_async(self, blocks, callback);
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.size)
    }
}

impl Drop for ObjectReader {
    fn drop(&mut self) {
        // If writer created it but dropped without marking for checkpoint or committing,
        // and it was not previously committed to remote store, clean up staged state.
        if !self.marked_for_checkpoint.load(Ordering::Relaxed)
            && !self.is_committed.load(Ordering::Relaxed)
        {
            let mut guard = self.content.lock().unwrap();
            *guard = None;
        }
    }
}

/// A writer that stages data blocks for object storage.
pub struct ObjectWriter {
    backend: ObjectStorageBackend,
    path: StoragePath,
    file_id: FileId,
    buffer: FBuf,
}

impl ObjectWriter {
    pub fn new(backend: ObjectStorageBackend, path: StoragePath) -> Self {
        Self {
            backend,
            path,
            file_id: FileId::new(),
            buffer: FBuf::new(),
        }
    }
}

impl FileRw for ObjectWriter {
    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn path(&self) -> &StoragePath {
        &self.path
    }
}

impl FileWriter for ObjectWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let arc_data = Arc::new(data);
        self.buffer.extend_from_slice(arc_data.as_slice());
        Ok(arc_data)
    }

    fn complete(self: Box<Self>) -> Result<Arc<dyn FileReader>, StorageError> {
        let size = self.buffer.len() as u64;
        let reader = Arc::new(ObjectReader {
            file_id: self.file_id,
            path: self.path.clone(),
            backend: self.backend.clone(),
            content: Mutex::new(Some(Arc::new(self.buffer))),
            marked_for_checkpoint: AtomicBool::new(false),
            is_committed: AtomicBool::new(false),
            size,
        });

        self.backend
            .active_readers
            .write()
            .unwrap()
            .insert(self.path, reader.clone());

        Ok(reader)
    }
}

/// Object storage backend implementing [StorageBackend].
#[derive(Clone)]
pub struct ObjectStorageBackend {
    store: Arc<DynObjectStore>,
    usage: Arc<AtomicI64>,
    active_readers: Arc<RwLock<HashMap<StoragePath, Arc<ObjectReader>>>>,
}

impl ObjectStorageBackend {
    /// Creates a new `ObjectStorageBackend` wrapping an `ObjectStore`.
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        Self {
            store,
            usage: Arc::new(AtomicI64::new(0)),
            active_readers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates an in-memory object storage backend (useful for testing and low-overhead mocks).
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemory::new()))
    }

    /// Creates an `ObjectStorageBackend` from an [`ObjectStorageConfig`].
    pub fn from_config(config: &ObjectStorageConfig) -> Result<Self, StorageError> {
        let url_str = config.url.trim();
        if url_str.is_empty() {
            return Ok(Self::in_memory());
        }

        let url = url::Url::parse(url_str)
            .map_err(|e| StorageError::InvalidURL(format!("{}: {}", url_str, e)))?;

        let (store, _path) = object_store::parse_url_opts(&url, &config.other_options)
            .map_err(StorageError::from)?;

        Ok(Self::new(Arc::from(store)))
    }
}

impl StorageBackend for ObjectStorageBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        Ok(Box::new(ObjectWriter::new(self.clone(), name.clone())))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        // First check active in-memory cache/staged readers.
        {
            let guard = self.active_readers.read().unwrap();
            if let Some(reader) = guard.get(name) {
                return Ok(reader.clone());
            }
        }

        // Fetch head metadata from underlying object store.
        let store = self.store.clone();
        let path = name.clone();

        let meta = TOKIO.block_on(async move {
            store.head(&path).await.map_err(StorageError::from)
        })?;

        let reader = Arc::new(ObjectReader {
            file_id: FileId::new(),
            path: name.clone(),
            backend: self.clone(),
            content: Mutex::new(None),
            marked_for_checkpoint: AtomicBool::new(true),
            is_committed: AtomicBool::new(true),
            size: meta.size as u64,
        });

        Ok(reader)
    }

    fn list(&self, parent: &StoragePath, cb: &mut dyn FnMut(DirEntry)) -> Result<(), StorageError> {
        use futures::StreamExt;

        let store = self.store.clone();
        let prefix = parent.clone();

        let entries = TOKIO.block_on(async move {
            let mut list_stream = store.list(Some(&prefix));
            let mut results = Vec::new();

            while let Some(item) = list_stream.next().await {
                match item {
                    Ok(meta) => {
                        results.push((meta.location, meta.size as u64));
                    }
                    Err(e) => {
                        return Err(StorageError::from(e));
                    }
                }
            }
            Ok(results)
        })?;

        for (location, size) in entries {
            cb(DirEntry {
                name: location,
                file_type: Ok(StorageFileType::File { size }),
            });
        }

        Ok(())
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        self.active_readers.write().unwrap().remove(name);

        let store = self.store.clone();
        let path = name.clone();

        TOKIO.block_on(async move {
            store.delete(&path).await.map_err(StorageError::from)
        })?;

        Ok(())
    }

    fn delete_recursive(&self, parent: &StoragePath) -> Result<(), StorageError> {
        use futures::StreamExt;

        self.active_readers
            .write()
            .unwrap()
            .retain(|p, _| !p.prefix_matches(parent));

        let store = self.store.clone();
        let prefix = parent.clone();

        TOKIO.block_on(async move {
            let mut list_stream = store.list(Some(&prefix));
            while let Some(item) = list_stream.next().await {
                if let Ok(meta) = item {
                    let _ = store.delete(&meta.location).await;
                }
            }
        });

        Ok(())
    }

    fn usage(&self) -> Arc<AtomicI64> {
        self.usage.clone()
    }
}

pub struct ObjectStorageBackendFactoryImpl;

impl StorageBackendFactory for ObjectStorageBackendFactoryImpl {
    fn backend(&self) -> &'static str {
        "object_store"
    }

    fn create(
        &self,
        _storage_config: &StorageConfig,
        backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        match backend_config {
            StorageBackendConfig::Object(config) => {
                let backend = ObjectStorageBackend::from_config(config)?;
                Ok(Arc::new(backend))
            }
            StorageBackendConfig::Default => {
                let backend = ObjectStorageBackend::in_memory();
                Ok(Arc::new(backend))
            }
            other => Err(StorageError::InvalidBackendConfig {
                backend: "object_store".to_string(),
                config: Box::new(other.clone()),
            }),
        }
    }
}

inventory::submit! {
    &ObjectStorageBackendFactoryImpl as &'static dyn StorageBackendFactory
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoragePath;

    #[test]
    fn test_object_store_memory_read_write() {
        let backend = ObjectStorageBackend::in_memory();
        let path: StoragePath = "checkpoints/test_file.dbsp".into();

        let mut writer = backend.create_named(&path).unwrap();
        let test_data = b"Hello, Feldera Object Storage!";
        writer.write_block(FBuf::from_slice(test_data)).unwrap();
        let reader = writer.complete().unwrap();

        reader.mark_for_checkpoint();
        reader.commit().unwrap();

        assert_eq!(reader.get_size().unwrap(), test_data.len() as u64);

        let block = reader
            .read_block(BlockLocation {
                offset: 0,
                size: test_data.len(),
            })
            .unwrap();
        assert_eq!(block.as_slice(), test_data);

        // Open newly created file from backend.
        let reader2 = backend.open(&path).unwrap();
        let block2 = reader2
            .read_block(BlockLocation {
                offset: 0,
                size: test_data.len(),
            })
            .unwrap();
        assert_eq!(block2.as_slice(), test_data);

        // List files.
        let mut listed = Vec::new();
        backend
            .list(&"checkpoints".into(), &mut |entry| {
                listed.push(entry.name);
            })
            .unwrap();
        assert!(listed.contains(&path));

        // Delete.
        backend.delete(&path).unwrap();
        assert!(!backend.exists(&path).unwrap());
    }
}
