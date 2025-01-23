from slack_sdk import WebClient
from matplotlib import pyplot
from utility.hashicorp import Vault
from utility.server.util import tmp_file
from typing import Dict, Optional

SLACK_COLORS = {
    'green': '#008000',
    'red': '#ff0000',
    'yellow': '#ffff00',
    'gray': '#808080',
    'black': '#000000',
    'orange': '#ff5700',
    'dusty_blue': '#8c9dad'
}


class Slack:
    _client = {}
    _channels = {}

    @classmethod
    def get_client(cls, bot_user: str = None) -> WebClient:
        if cls._client.get(bot_user, None) is None:
            cls._client[bot_user] = WebClient(token=Vault.get_secret(f"slack/{bot_user}")['key'])

        return cls._client[bot_user]

    @classmethod
    def get_channel_list(cls, bot_user: str) -> Dict:
        if cls._channels.get(bot_user, None) is None:
            data = cls.get_client(bot_user).conversations_list()
            cls._channels[bot_user] = {i['name']:i['id'] for i in data['channels']}
        return cls._channels[bot_user]

    @classmethod
    def get_channel_id(cls, bot_user: str, channel: str):
        if cls.get_channel_list(bot_user).get(channel, None) is None:
            raise Exception(f"Failed to retrieve channel id for {channel} while using token for {bot_user}")
        return cls.get_channel_list(bot_user)[channel]

    @classmethod
    def post_message(cls, bot_user: str, channel: str, text: str = None, **kwargs):
        channel_id = cls.get_channel_id(bot_user, channel)
        return cls.get_client(bot_user).chat_postMessage(channel=channel_id, text=text, **kwargs)

    @classmethod
    def post_file(cls, bot_user: str, channel: str, file: str = None,
                  content: str = None, filename: str = None, thread_ts: Optional[str] = None, **kwargs):
        channel_id = cls.get_channel_id(bot_user, channel)
        return cls.get_client(bot_user).files_upload_v2(channel=channel_id, file=file, content=content,
                                                        filename=filename, thread_ts=thread_ts, **kwargs)
    @classmethod
    def post_formatted_message(cls, bot_user, channel: str, content: str, fallback: str, color: str = None):
        params = {
            "attachments": [
                {
                    "mrkdwn_in": ["text"],
                    "text": content,
                    "fallback": fallback
                }
            ]
        }
        if color:
            params['attachments'][0]['color'] = SLACK_COLORS[color]

        return cls.post_message(bot_user=bot_user, channel=channel,  **params)

    @classmethod
    def post_plot(cls, bot_user: str, channel: str, plot: pyplot.Figure, filename: str = 'plot.png', thread_ts: str = None):
        with tmp_file() as plt_file:
            plot.savefig(plt_file, format='png')
            r = cls.post_file(
                bot_user=bot_user,
                channel=channel,
                file=plt_file,
                filename=filename,
                thread_ts=thread_ts
            )
        return r

if __name__ == '__main__':
    response = Slack.post_formatted_message('prefect', 'deals', '```Testing```', 'Test Tile', 'gray')
