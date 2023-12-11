# import __init__ as translate_to_hamilton
import __init__ as translate_to_hamilton

print(dir(translate_to_hamilton))


def test_code_segments():
    response = '''To translate this simple procedural code into Hamilton, you would define a single function representing the operation of adding `b` and `c` to produce `a`. Here's how it might look:

In `functions.py`, define the function:

```python
def a(b: float, c: float) -> float:
    """Adds b and c to get a."""
    return b + c
```

Then in `run.py`, you'd set up the Hamilton driver and execute the data flow:

```python
from hamilton import driver
import functions

dr = driver.Driver(config={}, module=functions)
result = dr.execute(["a"], inputs={"b": 1, "c": 2})  # assuming you're providing b=1, c=2 as inputs
print(result['a'])  # This will print 3, the result of addition
```

This Hamilton setup assumes that `b` and `c` are provided to the framework as inputs. If `b` and `c` were to be computed by other functions within the Hamilton framework or came from some form of data loading functions, those functions would need to be defined in `functions.py` as well, with appropriate signatures.
'''
    expected = [
        "def a(b: float, c: float) -> float:\n"
        '    """Adds b and c to get a."""\n'
        "    return b + c\n",
        "from hamilton import driver\n"
        "import functions\n"
        "\n"
        "dr = driver.Driver(config={}, module=functions)\n"
        'result = dr.execute(["a"], inputs={"b": 1, "c": 2})  # assuming you\'re '
        "providing b=1, c=2 as inputs\n"
        "print(result['a'])  # This will print 3, the result of addition\n",
    ]
    actual = translate_to_hamilton.code_segments(response)
    assert actual == expected
