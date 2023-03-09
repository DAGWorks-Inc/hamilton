from hamilton import settings


def test_power_mode_constant_invariant():
    assert settings.ENABLE_POWER_USER_MODE == "hamilton.enable_power_user_mode"
