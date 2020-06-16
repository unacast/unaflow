from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


def slack_message_on_context(http_conn_id: str = None,
                             webhook_token: str = None,
                             message: str = "",
                             attachments=None,
                             channel: str = None,
                             username: str = None,
                             icon_emoji: str = None,
                             icon_url: str = None,
                             link_names=False,
                             blocks=None,
                             proxy=None):
    """
    This is a wrapper function around the SlackWebhookOperator
     You can for example utilise it as a failure or retry function.
     Example:
     ...
     on_retry_callback=slack_message_on_context(message="Sensor timeout, we are doing another wait.")
     ...

    :param http_conn_id: connection that has Slack webhook token in the extra field
    :type http_conn_id: str
    :param webhook_token: Slack webhook token
    :type webhook_token: str
    :param message: The message you want to send on Slack
    :type message: str
    :param attachments: The attachments to send on Slack. Should be a list of
                        dictionaries representing Slack attachments.
    :type attachments: list
    :param channel: The channel the message should be posted to
    :type channel: str
    :param username: The username to post to slack with
    :type username: str
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :type icon_emoji: str
    :param icon_url: The icon image URL string to use in place of the default icon.
    :type icon_url: str
    :param link_names: Whether or not to find and link channel and usernames in your
                       message
    :type link_names: bool
    :param blocks: The blocks to send on Slack. Should be a list of
        dictionaries representing Slack blocks. (Only Airflow > 1.10.7)
    :type blocks: list
    :param proxy: Proxy to use to make the Slack webhook call
    :type proxy: str
    :return: a function that sends a Slack message using the SlackWebhookOperator
    """

    def on_context(context):
        retry_warning = SlackWebhookOperator(
            task_id='slack_failed_alert',
            http_conn_id=http_conn_id,
            webhook_token=webhook_token,
            message=message.format(**context),
            username=username,
            icon_emoji=icon_emoji,
            icon_url=icon_url,
            link_names=link_names,
            proxy=proxy,
            channel=channel,
            blocks=blocks,
            attachments=attachments
        )

        return retry_warning.execute(context)

    return on_context
