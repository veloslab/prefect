from utility.server.util import ssh_connection
from typing import List


def scp(source: str, remote: str, ssh_secret: str) -> None:
    """
    Transfer file via scp
    :param source: Local File
    :param remote: Remote File
    :param ssh_secret: Vault secret to SSH connection details such as host, user, private_key
    :return:
    """
    with ssh_connection(ssh_secret) as conn:
        conn.put(source, remote)


def remote_ls(path: str, ssh_secret: str) -> List:
    """
    List files on remote server
    :param path: Directory path on remote host
    :param ssh_secret: Vault secret to SSH connection details such as host, user, private_key
    :return:
    """
    with ssh_connection(ssh_secret) as ssh_conn:
        result = ssh_conn.run(f"ls {path}", hide=True)
    return result.stdout.splitlines()
