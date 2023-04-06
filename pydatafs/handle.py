from typing import Generic, TypeVar, List, Tuple

from .files import FSEntity, Directory, File

__all__ = ('FSHandle',)

T = TypeVar("T", bound=FSEntity)

class FSHandle(Generic[T]):
    def __init__(self, entity: T):
        self.entity = entity

    @property
    def inode(self):
        return self.entity.inode

class DirectoryHandle(FSHandle[Directory]):
    def __init__(self, entity: Directory):
        super().__init__(entity)
        self.iterator = entity.get_children()
        self.buffer: List[Tuple[str, FSEntity]] = []

class FileHandle(FSHandle[File]):
    pass
