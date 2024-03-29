from typing import Any, Dict, Optional

from slack_sdk import WebClient

from hamilton.lifecycle import NodeExecutionHook
from hamilton.lifecycle.default import NodeFilter, should_run_node


class SlackNotifier(NodeExecutionHook):
    """This is a adapter that sends a message to a slack channel when a node is executed.

    Note: you need to have slack_sdk installed for this to work.
    If you don't have it installed, you can install it with `pip install slack_sdk`
    (or `pip install sf-hamilton[slack]` -- use quotes if you're using zsh).

    .. code-block:: python

        from hamilton.plugins import h_slack

        dr = (
            driver.Builder()
            .with_config({})
            .with_modules(some_modules)
            .with_adapters(h_slack.SlackNotifier(api_key="YOUR_API_KEY", channel="YOUR_CHANNEL"))
            .build()
        )
        # and then when you call .execute() or .materialize() you'll get a message in your slack channel!

    """

    def __init__(self, api_key: str, channel: str, node_filter: NodeFilter = None):
        """Constructor.

        :param api_key: API key to use for sending messages.
        :param channel: Channel to send messages to.
        :param node_filter: Filter for nodes to send messages for.
        """
        self.slack_client = WebClient(api_key)
        self.channel = channel
        if node_filter is None:
            node_filter = lambda node_name, node_tags: node_name  # noqa E731
        self.node_filter = node_filter

    def send_message(self, message: str):
        """Sends a message to the slack channel."""
        if self.slack_client is not None:
            self.slack_client.chat_postMessage(channel=self.channel, text=message)

    def run_before_node_execution(
        self,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: Optional[str],
        run_id: str,
        node_input_types: Dict[str, Any],
        **future_kwargs: Any,
    ):
        """Sends a message to the slack channel before a node is executed."""
        if should_run_node(node_name, node_tags, self.node_filter):
            message = f"Executing node: {node_name}."
            if task_id is not None:
                message += f" Task ID: {task_id}."
            if run_id is not None:
                message += f" Run ID: {run_id}."
            self.send_message(message)

    def run_after_node_execution(
        self,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Optional[Exception],
        success: bool,
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        """Sends a message to the slack channel after a node is executed."""
        if should_run_node(node_name, node_tags, self.node_filter):
            message = f"Finished Executed node: {node_name}."
            if task_id is not None:
                message += f" Task ID: {task_id}."
            if run_id is not None:
                message += f" Run ID: {run_id}."
            if success:
                message += f" Result: {result}"
            if error is not None:
                message += f" Error: {error}"
            self.send_message(message)
