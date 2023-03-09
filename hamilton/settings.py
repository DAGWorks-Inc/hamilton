import json
import os

power_user_mode = False


def enable_power_user_mode():
    global power_user_mode
    power_user_mode = True


def is_power_user_mode_enabled():
    return json.loads(os.environ.get("HAMILTON_POWER_USER_MODE", "false")) or power_user_mode
