import json

from hamilton import driver

from tests.resources.dynamic_parallelism import no_parallel

EXPECTED_JSON = {
    "nodes": [
        {
            "name": "final",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["sum_step_squared_plus_step_cubed"],
            "optional_dependencies": [],
            "source": (
                "def final(sum_step_squared_plus_step_cubed: int) -> int:\n"
                '    print("finalizing")\n'
                "    return sum_step_squared_plus_step_cubed\n"
            ),
            "documentation": "",
        },
        {
            "name": "number_of_steps",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": [],
            "optional_dependencies": [],
            "source": "def number_of_steps() -> int:\n    return 5\n",
            "documentation": "",
        },
        {
            "name": "step_cubed",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["steps"],
            "optional_dependencies": [],
            "source": (
                "def step_cubed(steps: int) -> int:\n"
                '    print("cubing step {}".format(steps))\n'
                "    return steps**3\n"
            ),
            "documentation": "",
        },
        {
            "name": "step_squared",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["steps"],
            "optional_dependencies": [],
            "source": 'def step_squared(steps: int) -> int:\n    print("squaring step {}".format(steps))\n   '
            " return steps**2\n",
            "documentation": "",
        },
        {
            "name": "step_squared_plus_step_cubed",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["step_cubed", "step_squared"],
            "optional_dependencies": [],
            "source": (
                "def step_squared_plus_step_cubed(step_squared: int, step_cubed: int) -> int:\n"
                '    print("adding step squared and step cubed")\n'
                "    return step_squared + step_cubed\n"
            ),
            "documentation": "",
        },
        {
            "name": "steps",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["number_of_steps"],
            "optional_dependencies": [],
            "source": "def steps(number_of_steps: int) -> int:\n    return number_of_steps\n",
            "documentation": "",
        },
        {
            "name": "sum_step_squared_plus_step_cubed",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["step_squared_plus_step_cubed"],
            "optional_dependencies": [],
            "source": (
                "def sum_step_squared_plus_step_cubed(step_squared_plus_step_cubed: int) -> int:\n"
                '    print("summing step squared")\n'
                "    return step_squared_plus_step_cubed\n"
            ),
            "documentation": "",
        },
    ],
}


def test_export_execution():
    dr = driver.Builder().with_modules(no_parallel).build()
    json_str = dr.export_execution(["final"])
    assert json_str == json.dumps(EXPECTED_JSON)
