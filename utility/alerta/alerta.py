from alertaclient.api import Client, Alert
from utility.hashicorp.vault import Vault
from typing import List, Dict


class Alerta:
    client: Client = None
    debug: bool = False

    @classmethod
    def create_client(cls, endpoint: str, key: str = None) -> Client:
        if cls.client is None:
            Alerta.client = Client(endpoint=endpoint, key=key, debug=cls.debug)
        return cls.client

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
                   severity: str = None,
                   value: str = None,
                   attributes: Dict = None,
                   timeout: int = None):
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

        print(data)

if __name__ == '__main__':
    Alerta.send_alert('test', 'ters', 'eafd')

