import json

from hamilton import driver, graph_types

from tests.resources.dynamic_parallelism import no_parallel

# `version` value isn't hardcoded because it's Python version-dependent
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
            "version": None,  # `version` value isn't hardcoded because it's Python version-dependent
        },
        {
            "name": "number_of_steps",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": [],
            "optional_dependencies": [],
            "source": "def number_of_steps() -> int:\n    return 5\n",
            "documentation": "",
            "version": None,
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
            "version": None,
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
            "version": None,
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
            "version": None,
        },
        {
            "name": "steps",
            "tags": {"module": "tests.resources.dynamic_parallelism.no_parallel"},
            "output_type": "int",
            "required_dependencies": ["number_of_steps"],
            "optional_dependencies": [],
            "source": "def steps(number_of_steps: int) -> int:\n    return number_of_steps\n",
            "documentation": "",
            "version": None,
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
            "version": None,
        },
    ],
}


def test_export_execution():
    dr = driver.Builder().with_modules(no_parallel).build()
    graph = graph_types.HamiltonGraph.from_graph(dr.graph)

    # can't hard code HamiltonNode.version because it depends on Python version
    for n in graph.nodes:
        for json_n in EXPECTED_JSON["nodes"]:
            if json_n["name"] == n.name:
                json_n["version"] = n.version

    json_str = dr.export_execution(["final"])
    assert json_str == json.dumps(EXPECTED_JSON)
