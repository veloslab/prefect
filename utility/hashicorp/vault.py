import os
import hvac
import requests
from typing import Dict
from logging import getLogger
logger = getLogger('hashicorp.vault')


class Vault:
    _client: hvac.Client = None

    @classmethod
    def client(cls) -> hvac.Client:
        if cls._client is None:
            cls.authenticate()
        return cls._client

    @classmethod
    def authenticate(cls, url: str = None, token: str = None, role_id: str = None, secret_id: str = None):
        url = url or os.environ['VAULT_ADDR']
        if os.path.exists('/etc/ssl/certs/ca-certificates.crt'):
            client = hvac.Client(url=url, verify='/etc/ssl/certs/ca-certificates.crt')
        else:
            logger.warning("/etc/ssl/certs/ca-certificates.crt doesn't exist, using verify=True instead")
            client = hvac.Client(url=url, verify=True)
        token = token or os.environ.get('VAULT_TOKEN', None)
        if token:
            logger.debug("Using token for authentication")
            client.token = token
        else:
            logger.debug("Using approle for authentication")
            role_id = role_id or os.environ.get('VAULT_ROLE_ID', None)
            secret_id = secret_id or os.environ.get('VAULT_SECRET_ID', None)
            client.auth.approle.login(
                secret_id=secret_id,
                role_id=role_id
            )
        if not client.is_authenticated():
            raise Exception("Failed to authenticate with vault")

        logger.debug("Authenticated")

        cls._client = client

        return True

    @classmethod
    def get_static_database_credentials(cls, role: str, mount: str = 'database') -> Dict:
        """
        Get static database credential from vault
        :param role: Role for credential
        :param mount: Mount for database engine
        :return: Dictionary with username and password
        """
        response = cls.client().secrets.database.get_static_credentials(role, mount_point=mount)
        logger.info(f"Retrieved static-credential '{role}'")
        return {
            'username': response['data']['username'],
            'password': response['data']['password'],
        }

    @classmethod
    def get_secret(cls, path: str, mount: str = 'secret'):
        """
        Get secret from vault, only supports kv2 secret engine
        :param path: Path for secret
        :param mount: Mount for kv2 engine
        :return: Dictionary with username and password
        """
        response = cls.client().secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount
        )
        logger.info(f"Retrieved secret '{path}'")
        return response['data']['data']

    @classmethod
    def generate_backup(cls) -> requests.Response:
        logger.info(f"Initiated secret raft backup")
        response = cls.client().sys.take_raft_snapshot()
        logger.info(f"Retrieved secret raft backup")
        return response

if __name__ == "__main__":
    Vault.get_secret('reddit')