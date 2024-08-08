import hamilton_anthropic
from hamilton import driver

anthropic_driver = (
    driver.Builder().with_modules(hamilton_anthropic).with_config({"provider": "anthropic"}).build()
)
openai_driver = (
    driver.Builder().with_modules(hamilton_anthropic).with_config({"provider": "openai"}).build()
)
try:
    print(anthropic_driver.execute(["joke_response"], inputs={"topic": "ice cream"}))
except Exception:
    # this is the current way to do fall backs
    print(openai_driver.execute(["joke_response"], inputs={"topic": "ice cream"}))
