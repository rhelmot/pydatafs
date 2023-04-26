from typing import TYPE_CHECKING, Dict, Optional, Tuple, TypeVar, cast
import errno
import functools
import logging
import os

import pyfuse3
import pyfuse3_asyncio

from .files import Directory, File, FSEntity, Symlink
from .handle import DirectoryHandle, FileHandle, FSHandle

if TYPE_CHECKING:
    from pyfuse3 import FileHandleT, FileNameT, FlagT, InodeT, ModeT
else:
    InodeT = int
    FileHandleT = int
    FileNameT = bytes
    FlagT = int
    ModeT = int

T = TypeVar("T", bound=FSEntity)

log = logging.getLogger(__name__)


def async_fuse_condom(f):
    @functools.wraps(f)
    async def inner(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except pyfuse3.FUSEError:
            raise
        except Exception:
            log.exception("")
            raise pyfuse3.FUSEError(errno.EIO)

    return inner


class PyDataFS(pyfuse3.Operations):
    def __init__(
        self,
        name: str,
        root_dir: Directory,
        uid: Optional[int] = None,
        gid: Optional[int] = None,
        debug: bool = False,
        encoding: str = "utf-8",
    ):
        self.name = name
        self.root_dir = root_dir
        self.uid = uid if uid is not None else os.getuid()
        self.gid = gid if gid is not None else os.getgid()
        self.debug = debug
        self.encoding = encoding

        self.next_inode = pyfuse3.ROOT_INODE
        self.next_handle = FileHandleT(1)
        self.entity_lookup: Dict[InodeT, FSEntity] = {}
        self.entity_dedup: Dict[FSEntity, FSEntity] = {}
        self.handle_lookup: Dict[FileHandleT, FSHandle] = {}
        self._onboard(self.root_dir, self.root_dir)
        assert self.root_dir.inode == pyfuse3.ROOT_INODE

    async def run(self, mountpoint: str):
        pyfuse3_asyncio.enable()
        options = set(pyfuse3.default_options)
        options.add(f"fsname={self.name}")
        # options.add(f"uid={self.uid}")
        # options.add(f"gid={self.gid}")
        if self.debug:
            options.add("debug")
        pyfuse3.init(self, mountpoint, options)
        try:
            await pyfuse3.main()
        finally:
            pyfuse3.close()

    def alloc_inode(self) -> InodeT:
        result = self.next_inode
        self.next_inode = InodeT(self.next_inode + 1)
        return result

    def alloc_handle(self) -> FileHandleT:
        self.next_handle = FileHandleT(self.next_handle + 1)
        return self.next_handle

    def _onboard(self, parent: Directory, entity: T) -> T:
        entity = cast(T, self.entity_dedup.get(entity, entity))
        if entity._inode is None:
            entity._inode = self.alloc_inode()
            entity._parent = parent
            self.entity_lookup[entity._inode] = entity
            self.entity_dedup[entity] = entity
        return entity

    @async_fuse_condom
    async def getattr(self, inode, ctx=None) -> pyfuse3.EntryAttributes:
        entity = self.entity_lookup.get(inode, None)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return await self._getattr(entity)

    async def _getattr(self, entity: FSEntity) -> pyfuse3.EntryAttributes:
        result = pyfuse3.EntryAttributes()
        result.st_ino = entity.inode
        result.st_uid = self.uid
        result.st_gid = self.gid
        result.st_mode = entity.mode
        result.st_size = await entity.size()
        return result

    @async_fuse_condom
    async def setattr(
        self,
        inode: InodeT,
        attr: pyfuse3.EntryAttributes,
        fields: pyfuse3.SetattrFields,
        fh: Optional[FileHandleT],
        ctx: pyfuse3.RequestContext,
    ) -> pyfuse3.EntryAttributes:
        entity = self.entity_lookup.get(inode, None)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if fields.update_size:
            if not isinstance(entity, File):
                raise pyfuse3.FUSEError(errno.EINVAL)
            await entity.truncate(attr.st_size)

        return await self._getattr(entity)

    async def _lookup(self, parent_inode: InodeT, name: FileNameT, ctx=None) -> FSEntity:
        parent_entity = self.entity_lookup.get(parent_inode, None)
        if parent_entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(parent_entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        try:
            name_str = name.decode(self.encoding)
        except UnicodeDecodeError:
            raise pyfuse3.FUSEError(errno.ENOENT) from None

        if name_str == ".":
            return parent_entity

        if name_str == "..":
            return parent_entity.parent

        entity = await parent_entity.get_child(name_str)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        entity = self._onboard(parent_entity, entity)
        return entity

    @async_fuse_condom
    async def lookup(self, parent_inode, name, ctx=None) -> pyfuse3.EntryAttributes:
        entity = await self._lookup(parent_inode, name)
        entity.lookup_count += 1
        return await self._getattr(entity)

    @async_fuse_condom
    async def forget(self, inode_lst) -> None:
        for inode, nlookup in inode_lst:
            parent_entity = self.entity_lookup.get(inode, None)
            if parent_entity is not None:
                parent_entity.lookup_count -= nlookup
                if parent_entity.lookup_count <= 0:
                    if parent_entity.lookup_count < 0:
                        print("Bug: negative reference count")
                    self.entity_lookup.pop(inode)

    @async_fuse_condom
    async def opendir(self, inode, ctx) -> FileHandleT:
        entity = self.entity_lookup.get(inode, None)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        fh = self.alloc_handle()
        handle = DirectoryHandle(entity)
        self.handle_lookup[fh] = handle
        return fh

    @async_fuse_condom
    async def readdir(self, fh, start_id, token) -> None:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        while True:
            if start_id == 0:
                name = "."
                entity = handle.entity
            elif start_id == 1:
                name = ".."
                entity = handle.entity.parent
            else:
                if start_id - 2 >= len(handle.buffer):
                    async for name, child_entity in handle.iterator:
                        child_entity = self._onboard(handle.entity, child_entity)
                        handle.buffer.append((name, child_entity))
                        if start_id - 2 >= len(handle.buffer):
                            break
                    else:
                        break
                name, entity = handle.buffer[start_id - 2]
            start_id += 1
            if not pyfuse3.readdir_reply(
                token, FileNameT(name.encode(self.encoding)), await self._getattr(entity), start_id
            ):
                break
            if name not in (".", ".."):
                entity.lookup_count += 1

    @async_fuse_condom
    async def releasedir(self, fh) -> None:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        self.handle_lookup.pop(fh)

    @async_fuse_condom
    async def open(self, inode, flags, ctx) -> pyfuse3.FileInfo:
        entity = self.entity_lookup.get(inode, None)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if isinstance(entity, Directory):
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not isinstance(entity, File):
            raise pyfuse3.FUSEError(errno.EINVAL)

        fh = self.alloc_handle()
        handle = FileHandle(entity)
        self.handle_lookup[fh] = handle

        if flags & os.O_TRUNC != 0:
            await entity.truncate(0)

        result = pyfuse3.FileInfo()
        result.fh = fh
        result.direct_io = True
        return result

    @async_fuse_condom
    async def read(self, fh: FileHandleT, off, size) -> bytes:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not isinstance(handle, FileHandle):
            raise pyfuse3.FUSEError(errno.EINVAL)

        return await handle.entity.read(off, size)

    @async_fuse_condom
    async def write(self, fh: FileHandleT, off: int, buf: bytes) -> int:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not isinstance(handle, FileHandle):
            raise pyfuse3.FUSEError(errno.EINVAL)

        return await handle.entity.write(off, buf)

    @async_fuse_condom
    async def flush(self, fh: FileHandleT) -> None:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not isinstance(handle, FileHandle):
            raise pyfuse3.FUSEError(errno.EINVAL)

        await handle.entity.flush()

    @async_fuse_condom
    async def release(self, fh) -> None:
        handle = self.handle_lookup.get(fh, None)
        if handle is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if isinstance(handle, DirectoryHandle):
            raise pyfuse3.FUSEError(errno.EISDIR)

        self.handle_lookup.pop(fh)

    @async_fuse_condom
    async def readlink(self, inode: InodeT, ctx=None) -> FileNameT:
        entity = self.entity_lookup.get(inode, None)
        if entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(entity, Symlink):
            raise pyfuse3.FUSEError(errno.EINVAL)

        result = await entity.readlink()
        return FileNameT(result.encode(self.encoding))

    @async_fuse_condom
    async def create(
        self,
        parent_inode: InodeT,
        name: FileNameT,
        mode: ModeT,
        flags: FlagT,
        ctx: pyfuse3.RequestContext,
    ) -> Tuple[pyfuse3.FileInfo, pyfuse3.EntryAttributes]:
        parent_entity = self.entity_lookup.get(parent_inode, None)
        if parent_entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(parent_entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        try:
            name_str = name.decode(self.encoding)
        except UnicodeDecodeError:
            raise pyfuse3.FUSEError(errno.ENOENT) from None

        entity = await parent_entity.create(name_str, mode, flags)
        entity = self._onboard(parent_entity, entity)

        fh = self.alloc_handle()
        handle = FileHandle(entity)
        self.handle_lookup[fh] = handle
        attrs = await self._getattr(entity)

        fi = pyfuse3.FileInfo()
        fi.fh = fh
        fi.direct_io = True
        return (fi, attrs)

    @async_fuse_condom
    async def mkdir(
        self, parent_inode: InodeT, name: FileNameT, mode: ModeT, ctx: pyfuse3.RequestContext
    ) -> pyfuse3.EntryAttributes:
        parent_entity = self.entity_lookup.get(parent_inode, None)
        if parent_entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(parent_entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        try:
            name_str = name.decode(self.encoding)
        except UnicodeDecodeError:
            raise pyfuse3.FUSEError(errno.ENOENT) from None

        entity = await parent_entity.mkdir(name_str, mode)
        attrs = await self._getattr(entity)
        return attrs

    @async_fuse_condom
    async def unlink(self, parent_inode: InodeT, name: FileNameT, ctx: pyfuse3.RequestContext) -> None:
        parent_entity = self.entity_lookup.get(parent_inode, None)
        if parent_entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(parent_entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        try:
            name_str = name.decode(self.encoding)
        except UnicodeDecodeError:
            raise pyfuse3.FUSEError(errno.ENOENT) from None

        await parent_entity.unlink_child(name_str)

    @async_fuse_condom
    async def rmdir(self, parent_inode: InodeT, name: FileNameT, ctx: pyfuse3.RequestContext) -> None:
        parent_entity = self.entity_lookup.get(parent_inode, None)
        if parent_entity is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not isinstance(parent_entity, Directory):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        try:
            name_str = name.decode(self.encoding)
        except UnicodeDecodeError:
            raise pyfuse3.FUSEError(errno.ENOENT) from None

        await parent_entity.unlink_child(name_str)
