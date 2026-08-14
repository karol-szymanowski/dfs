use crate::inode::InodeTable;
use gfs_client::GfsClient;
use std::sync::Arc;

#[cfg(target_os = "linux")]
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    Request,
};
#[cfg(target_os = "linux")]
use std::ffi::OsStr;
#[cfg(target_os = "linux")]
use std::time::{Duration, UNIX_EPOCH};

#[cfg(target_os = "linux")]
const TTL: Duration = Duration::from_secs(1);

pub struct GfsFilesystem {
    pub client: GfsClient,
    pub runtime_handle: tokio::runtime::Handle,
    pub inodes: Arc<InodeTable>,
}

impl GfsFilesystem {
    pub fn new(client: GfsClient, runtime_handle: tokio::runtime::Handle) -> Self {
        Self {
            client,
            runtime_handle,
            inodes: Arc::new(InodeTable::new()),
        }
    }
}

#[cfg(target_os = "linux")]
impl Filesystem for GfsFilesystem {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file_name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let target_path = parent_path.join(file_name);
        let ino = self.inodes.get_or_insert(&target_path);

        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        };

        reply.entry(&TTL, &attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let is_root = ino == 1;
        let attr = FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: if is_root {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: if is_root { 0o755 } else { 0o644 },
            nlink: if is_root { 2 } else { 1 },
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        };
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != 1 {
            reply.error(libc::ENOTDIR);
            return;
        }

        let entries = vec![
            (1, FileType::Directory, "."),
            (1, FileType::Directory, ".."),
        ];

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let path_str = path.to_string_lossy().to_string();
        let client = self.client.clone();

        let data = self
            .runtime_handle
            .block_on(async move { client.read(&path_str, offset as u64, size).await });

        match data {
            Ok(bytes) => reply.data(&bytes),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let path_str = path.to_string_lossy().to_string();
        let client = self.client.clone();
        let bytes = bytes::Bytes::copy_from_slice(data);

        let res = self
            .runtime_handle
            .block_on(async move { client.append(&path_str, bytes).await });

        match res {
            Ok(_) => reply.written(data.len() as u32),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file_name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let target_path = parent_path.join(file_name);
        let path_str = target_path.to_string_lossy().to_string();
        let client = self.client.clone();

        let res = self
            .runtime_handle
            .block_on(async move { client.delete(&path_str).await });

        match res {
            Ok(()) => {
                self.inodes.remove_by_path(&target_path);
                reply.ok();
            }
            Err(_) => reply.error(libc::EIO),
        }
    }
}
