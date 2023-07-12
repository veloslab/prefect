from utility.server.util import ssh_connection


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

