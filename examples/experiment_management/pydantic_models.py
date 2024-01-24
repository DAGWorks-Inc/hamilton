from typing import Any, Optional

from pydantic import BaseModel, create_model


def model_from_values(name: str, specs: dict[str, Any]) -> dict[str, tuple[type, ...]]:
    return create_model(name, **{k: (type(v), ...) for k, v in specs.items()})


class NodeImplementation(BaseModel):
    name: str
    source_code: str


class NodeInput(BaseModel):
    name: str
    value: Any
    default_value: Optional[Any]


class NodeOverride(BaseModel):
    name: str
    value: Any


class NodeMaterializer(BaseModel):
    source_nodes: list[str]
    path: str
    sink: str
    data_saver: str


class RunMetadata(BaseModel):
    run_id: str
    run_dir: str
    success: bool
    # date_completed: datetime.datetime
    graph_hash: str
    modules: list[str]
    inputs: list[NodeInput]
    overrides: list[NodeOverride]
    materialized: list[NodeMaterializer]


# def model_from_signature(name: str, function) -> dict[str, tuple[type, Any]]:
#     specs = dict()
#     for param_name, param in inspect.signature(function).parameters.items():
#         default = None if param.default is inspect._empty else param.default
#         specs[param_name] = (param.annotation, default)
#     return create_model(name, **specs)


# LaunchInputs = model_from_signature(
#     name="LaunchInputs",
#     function=runner.main
# )
