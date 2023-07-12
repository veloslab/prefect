import os
import tempfile
import contextlib
from utility.hashicorp import Vault
from fabric import Connection


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

