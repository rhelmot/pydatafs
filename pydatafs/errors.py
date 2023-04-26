import errno

from pyfuse3 import FUSEError


def FSPermissionError():
    return FUSEError(errno.EPERM)


def FSInvalidError():
    return FUSEError(errno.EINVAL)


def FSIsDirectoryError():
    return FUSEError(errno.EISDIR)


def FSNotDirectoryError():
    return FUSEError(errno.ENOTDIR)


def FSMissingError():
    return FUSEError(errno.ENOENT)


def FSUnimplementedError():
    return FUSEError(errno.ENOSYS)
