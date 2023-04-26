from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Union,
)
from asyncio import Lock
from datetime import datetime, timedelta
import errno
import stat

import pyfuse3

from .errors import FSInvalidError, FSMissingError

if TYPE_CHECKING:
    from pyfuse3 import FileHandleT, FileNameT, FlagT, InodeT, ModeT
else:
    InodeT = int
    FileHandleT = int
    FileNameT = bytes
    ModeT = int
    FlagT = int

__all__ = ("FSEntity", "File", "Directory", "Symlink")


class FSEntity:
    def __init__(self, mode: int = 0o644):
        self._inode: Optional[InodeT] = None
        self._parent: Optional["Directory"] = None
        self.lookup_count = 0
        self.mode = mode

    @property
    def inode(self) -> InodeT:
        if self._inode is None:
            raise ValueError("Entity is not bound to filesystem")
        return self._inode

    @property
    def parent(self) -> "Directory":
        if self._parent is None:
            raise ValueError("Entity is not bound to filesystem")
        return self._parent

    async def size(self) -> int:
        return 0

    async def unlink(self) -> None:
        raise FSInvalidError()

    def __eq__(self, other):
        raise NotImplementedError(type(self))


class File(FSEntity):
    def __init__(self, mode=0o644):
        mode |= stat.S_IFREG
        super().__init__(mode)

    async def read(self, off: int, size: int) -> bytes:
        raise NotImplementedError

    async def write(self, off: int, data: bytes) -> int:
        raise NotImplementedError

    async def flush(self) -> None:
        pass

    async def truncate(self, off: int):
        raise NotImplementedError


class RWBufferedFile(File):
    def __init__(
        self,
        identity,
        content: Union[bytes, Callable[[], Awaitable[bytearray]]],
        size: Optional[Callable[[], Awaitable[int]]] = None,
        writeback: Optional[Callable[[bytes], Awaitable[None]]] = None,
        mode: Optional[int] = None,
        timeout: timedelta = timedelta(seconds=30),
    ):
        if mode is None:
            mode = 0o444 if writeback is None else 0o644
        super().__init__(mode)

        self.identity = identity
        self._size_routine = size
        self._writeback_routine = writeback
        self._dirty = False
        self._expiration: Optional[datetime] = None
        self._timeout: timedelta = timeout
        self._lock = Lock()
        if isinstance(content, bytes):
            self._content: Optional[bytearray] = bytearray(content)
            self._content_routine: Optional[Callable[[], Awaitable[bytearray]]] = None
            self._size: Optional[int] = len(content)
        else:
            self._content = None
            self._content_routine = content
            self._size = None

    async def content(self) -> bytearray:
        assert self._lock.locked()
        if not self._dirty and self._expiration is not None:
            now = datetime.now()
            if now > self._expiration:
                self._content = None
                self._expiration = None

        if self._content is None:
            assert self._content_routine is not None
            self._content = await self._content_routine()
            self._dirty = False
            self._expiration = datetime.now() + self._timeout
        return self._content

    async def read(self, off: int, size: int) -> bytes:
        async with self._lock:
            content = await self.content()
            return content[off : off + size]

    async def write(self, off: int, data: bytes) -> int:
        async with self._lock:
            content = await self.content()
            content[off : off + len(data)] = data
            self._dirty = True
        return len(data)

    async def truncate(self, off: int):
        async with self._lock:
            if off == 0:
                self._content = bytearray()
                self._dirty = True
            else:
                self._content = (await self.content())[0:off]
                self._dirty = True

    async def flush(self):
        async with self._lock:
            if self._dirty:
                assert self._content is not None
                if self._writeback_routine:
                    try:
                        await self._writeback_routine(self._content)
                    except:
                        self._dirty = False
                        self._content = None
                        raise
                self._dirty = False

    async def size(self) -> int:
        async with self._lock:
            if self._size is None:
                if self._size_routine is not None:
                    self._size = await self._size_routine()
                else:
                    content = await self.content()
                    self._size = len(content)
            return self._size

    def __eq__(self, other):
        return isinstance(other, RWBufferedFile) and other.identity == self.identity

    def __hash__(self):
        return hash(self.identity)


class Directory(FSEntity):
    def __init__(self, mode: int = 0o755):
        mode |= stat.S_IFDIR
        super().__init__(mode)

    async def get_children(self) -> AsyncGenerator[Tuple[str, FSEntity], None]:
        raise NotImplementedError
        yield ...

    async def get_child(self, name: str) -> Optional[FSEntity]:
        async for child_name, child in self.get_children():
            if child_name == name:
                return child
        return None

    async def create(self, name: str, mode: ModeT, flags: FlagT) -> File:
        raise FSInvalidError()

    async def mkdir(self, name: str, mode: ModeT) -> "Directory":
        raise FSInvalidError()

    async def symlink(self, name: str, target: str) -> "Symlink":
        raise FSInvalidError()

    async def unlink_child(self, name: str):
        child = await self.get_child(name)
        if child is None:
            raise FSMissingError()
        await child.unlink()


class Symlink(FSEntity):
    def __init__(self, identity, target: str, mode: int = 0o777):
        self.identity = identity
        mode |= stat.S_IFLNK
        self.target = target
        super().__init__(mode)

    async def readlink(self) -> str:
        return self.target

    def __eq__(self, other):
        return isinstance(other, Symlink) and other.identity == self.identity

    def __hash__(self):
        return hash(self.identity)
