from b2sdk.v2 import InMemoryAccountInfo
from b2sdk.v2 import B2Api
from utility.hashicorp import Vault
from typing import Dict
creds = Vault.get_secret('backblaze/prefect-vault')


class Bucket:
    def __init__(self, name):
        creds = Vault.get_secret(f"backblaze/{name}")
        info = InMemoryAccountInfo()
        self._client: B2Api = B2Api(info)
        self._client.authorize_account("production", creds['key_id'], creds['application_key'])
        self._bucket = self._client.get_bucket_by_name(name)

    def upload_file(self, filepath, filename: str = None, file_info: Dict = None):
        self._bucket.upload(filepath, filename, file_info)
