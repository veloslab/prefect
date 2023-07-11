import os
import tempfile
import contextlib


@contextlib.contextmanager
def tmp_file(content: str, prefix: str = None) -> str:
    """
    Generate a file via context manager, file will be deleted on closure
    :param content: File Content
    :param prefix: Prefix for file
    :return: File path to temp file
    """
    fd, temp_path = tempfile.mkstemp(prefix=prefix)
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    try:
        yield temp_path
    finally:
        os.unlink(temp_path)
