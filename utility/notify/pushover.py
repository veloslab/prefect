import requests
from utility.hashicorp import Vault
from typing import Dict


class Pushover:
    _message_url: str = 'https://api.pushover.net/1/messages.json'
    __credentials = {}

    @classmethod
    def __get_credentials(cls, app: str)->Dict:
        """
        Pull pushover credentials from Vault
        :param app: Application channel name that has corresponding secret saved in Vault
        :return: Credentials
        """
        if cls.__credentials.get(app, None) is None:
            try:
                cls.__credentials[app] = Vault.get_secret(f"pushover/{app}")
            except Exception as e:
                raise ValueError(f"Unable pull secret 'pushover/{app}', review access and ensure secret exists")
        return cls.__credentials[app]

    @classmethod
    def send(cls, app: str, message: str,
             priority: int = None, title: str = None, url: str = None, url_title: str = None) -> requests.Response:
        """
        :param app: Application channel where message will be sent
        :param message: Message
        :param priority: A value of -2, -1, 0 (default), 1, or 2
        :param title: Message's title, otherwise your app's name is used
        :param url: Supplementary URL to show with your message
        :param url_title: Title for the URL specified as the url parameter, otherwise just the URL is shown
        :return:
        """
        auth = cls.__get_credentials(app)
        data = {
            "token": auth['app_token'],
            "user": auth['user_key'],
            "message": message
        }

        # Add optional params to payload
        if priority:
            data['priority'] = priority
        if title:
            data['title'] = title
        if url:
            data['url'] = url
        if url_title:
            data['url_title'] = url_title

        return requests.request('POST', url=cls._message_url, data=data)


