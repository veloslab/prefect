import os
import tempfile
import contextlib
from utility.hashicorp import Vault
from fabric import Connection
from typing import Union


@contextlib.contextmanager
def tmp_file(content: Union[str, bytes] = None, prefix: str = None, suffix: str = None) -> str:
    """
    Generate a file via context manager, file will be deleted on closure. If content is not full, file will be populated
    :param content: File Content
    :param prefix: Prefix for file
    :param suffix: Suffix for file
    :return: File path to temp file
    """
    fd, temp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    if content:
        with os.fdopen(fd, 'wb' if isinstance(content, bytes) else 'w') as f:
            f.write(content)


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


@contextlib.contextmanager
def ssh_connection(ssh_secret: str) -> Connection:
    """
    Generate a SSH Connection via context manager
    :param ssh_secret: Vault secret to SSH connection details such as host, user, private_key
    :return: SSH Connection
    """
    ssh_data = Vault.get_secret(ssh_secret)
    with tmp_file(ssh_data['private_key'], prefix='ssh_') as ssh_private_key:
        conn = Connection(host=ssh_data['host'],
                          user=ssh_data['user'],
                          connect_kwargs={
                              "key_filename": ssh_private_key
                          })
        yield conn
        conn.close()

