import os
import hvac
from typing import Dict


class Vault:
    client: hvac.Client = None

    @staticmethod
    def get_client() -> hvac.Client:
        if Vault.client is None:
            Vault.client = hvac.Client(url=os.environ['VAULT_ADDR'])
            Vault.client.auth.approle.login(
                role_id=os.environ['VAULT_ROLE_ID'],
                secret_id=os.environ['VAULT_SECRET_ID']
            )
        return Vault.client

    @staticmethod
    def get_static_database_credentials(role: str, mount: str = 'database') -> Dict:
        """
        Get static database credential from vault
        :param role: Role for credential
        :param mount: Mount for database engine
        :return: Dictionary with username and password
        """
        response = Vault.get_client().secrets.database.get_static_credentials(role, mount_point=mount)
        return {
            'username': response['data']['username'],
            'password': response['data']['password'],
        }

    @staticmethod
    def get_secret(path: str, mount: str = 'secret'):
        """
        Get secret from vault, only supports kv2 secret engine
        :param path: Path for secret
        :param mount: Mount for kv2 engine
        :return: Dictionary with username and password
        """
        response = Vault.get_client().secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount
        )
        return response['data']['data']