from alertaclient.api import Client, Alert
from utility.hashicorp import Vault
from typing import List, Dict


class Alerta:
    _client: Client = None

    @classmethod
    @property
    def client(cls, endpoint: str = None, key: str = None, debug: bool = False) -> Client:
        if cls._client is None:
            if endpoint and key:
                cls._client = Client(endpoint, key, debug=debug)
            else:
                credentials = Vault.get_secret('alerta/api/alert')
                cls._client = Client(endpoint=credentials['host'], key=credentials['key'], debug=debug)
        return cls._client

    @classmethod
    def send_alert(cls,
                   environment: str,
                   resource: str,
                   event: str,
                   text: str = None,
                   service: List = None,
                   correlate: List = None,
                   tags: List = None,
                   group: str = None,
                   severity: str = 'ok',
                   value: str = None,
                   attributes: Dict = None,
                   timeout: int = None) -> Alert:
        """
        Send Alert
        :param environment: Environment eg. Production, Development
        :param resource: Resource under alarm
        :param event: Event name
        :param text: Description of alert
        :param service: List of affected services eg. app name, Web, Network, Storage, Database, Security
        :param correlate: List of related events eg. node_up, node_down
        :param tags: List of tags eg. London, os:linux, AWS/EC2
        :param group: Group event by type eg. OS, Performance
        :param severity: Severity eg. critical, major, minor, warning
        :param value: Event value
        :param attributes: List of attributes eg. {'priority':'high'}
        :param timeout: Seconds before an open alert will be expired
        :return: Alert Object
        """
        parameters = locals()
        data = {}
        for key, value in parameters.items():
            if key == 'cls':
                continue
            if value:
                data[key] = value

        alert = cls.client.send_alert(**data)
        return alert
