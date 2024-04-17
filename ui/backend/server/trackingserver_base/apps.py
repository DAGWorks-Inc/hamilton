import os.path
import uuid

from django.apps import AppConfig
from hamilton.telemetry import is_telemetry_enabled, BASE_PROPERTIES, send_event_json, API_KEY


def create_server_event_json(telemetry_key: str) -> dict:
    """Function to create payload for tracking server event.

    :param event_name: the name of the server event
    :return: dict representing the JSON to send.
    """
    old_anonymous_id = BASE_PROPERTIES["distinct_id"]
    event = {
        "event": "os_hamilton_ui_server_start",
        "api_key": API_KEY,
        "properties" : {"telemetry_key": telemetry_key, "old_anonymous_id": old_anonymous_id}
    }
    event["properties"].update(BASE_PROPERTIES)
    event["properties"]["distinct_id"] = telemetry_key
    return event


class TrackingServerConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "trackingserver_base"

    def ready(self):
        if is_telemetry_enabled():
            if not os.path.exists("/data/telemetry.txt"):
                telemetry_key = str(uuid.uuid4())
                with open("/data/telemetry.txt", "w") as f:
                    f.write(telemetry_key)
            else:
                with open("/data/telemetry.txt", "r") as f:
                    telemetry_key = f.read().strip()
            send_event_json(create_server_event_json(telemetry_key))

