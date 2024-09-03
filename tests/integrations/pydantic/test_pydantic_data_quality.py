from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from hamilton.data_quality.pydantic_validators import PydanticModelValidator
from hamilton.function_modifiers import check_output
from hamilton.node import Node
from hamilton.plugins import h_pydantic


def test_basic_pydantic_validator_passes():
    class DummyModel(BaseModel):
        value: float

    validator = PydanticModelValidator(model=DummyModel, importance="warn")
    validation_result = validator.validate({"value": 15.0})
    assert validation_result.passes


def test_basic_pydantic_check_output_passes():
    class DummyModel(BaseModel):
        value: float

    @check_output(model=DummyModel, importance="warn")
    def dummy() -> dict[str, float]:
        return {"value": 15.0}

    node = Node.from_fn(dummy)
    validators = check_output(model=DummyModel).get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result_success = validator.validate(node())
    assert result_success.passes


def test_basic_pydantic_validator_fails():
    class DummyModel(BaseModel):
        value: float

    validator = PydanticModelValidator(model=DummyModel, importance="warn")
    validation_result = validator.validate({"value": "15.0"})
    assert not validation_result.passes
    assert "value" in validation_result.diagnostics["model_errors"][0]["loc"]


def test_basic_pydantic_check_output_fails():
    class DummyModel(BaseModel):
        value: float

    @check_output(model=DummyModel, importance="warn")
    def dummy() -> dict[str, float]:
        return {"value": "fifteen"}  # type: ignore

    node = Node.from_fn(dummy)
    validators = check_output(model=DummyModel).get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result = validator.validate(node())
    assert not result.passes


def test_pydantic_validator_is_strict():
    class DummyModel(BaseModel):
        value: float

    validator = PydanticModelValidator(model=DummyModel, importance="warn")
    validation_result = validator.validate({"value": "15"})
    assert not validation_result.passes


def test_complex_pydantic_validator_passes():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    data = {
        "name": "hamilton",
        "owner": {"name": "DAGWorks-Inc"},
        "versions": [{"name": "0.1.0", "id": 1}, {"name": "0.2.0", "id": 2}],
    }

    validator = PydanticModelValidator(model=Repo, importance="warn")
    validation_result = validator.validate(data)
    assert validation_result.passes


def test_complex_pydantic_validator_fails():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    data = {
        "name": "hamilton",
        "owner": {"name": "DAGWorks-Inc"},
        "versions": [{"name": "0.1.0", "id": 1}, {"name": "0.2.0", "id": "2"}],
    }

    validator = PydanticModelValidator(model=Repo, importance="warn")
    validation_result = validator.validate(data)
    assert not validation_result.passes


def test_complex_pydantic_check_output_passes():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    @check_output(model=Repo, importance="warn")
    def dummy() -> dict[str, Any]:
        return {
            "name": "hamilton",
            "owner": {"name": "DAGWorks-Inc"},
            "versions": [{"name": "0.1.0", "id": 1}, {"name": "0.2.0", "id": 2}],
        }

    node = Node.from_fn(dummy)
    validators = check_output(model=Repo).get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result_success = validator.validate(node())
    assert result_success.passes


def test_complex_pydantic_check_output_fails():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    @check_output(model=Repo, importance="warn")
    def dummy() -> dict[str, Any]:
        return {
            "name": "hamilton",
            "owner": {"name": "DAGWorks-Inc"},
            "versions": [
                {"name": "0.1.0", "id": "one"},  # id should be an int
                {"name": "0.2.0", "id": "two"},  # id should be an int
            ],
        }

    node = Node.from_fn(dummy)
    validators = check_output(model=Repo).get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result = validator.validate(node())
    assert not result.passes


def test_basic_pydantic_plugin_check_output_passes():
    class DummyModel(BaseModel):
        value: float

    def dummy() -> DummyModel:
        return DummyModel(value=15.0)

    node = Node.from_fn(dummy)
    validators = h_pydantic.check_output().get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result_success = validator.validate(node())
    assert result_success.passes


def test_basic_pydantic_plugin_check_output_fails():
    class DummyModel(BaseModel):
        value: float

    def dummy() -> DummyModel:
        return DummyModel(value="fifteen")  # type: ignore

    node = Node.from_fn(dummy)
    validators = h_pydantic.check_output().get_validators(node)
    assert len(validators) == 1
    validator = validators[0]

    with pytest.raises(ValidationError):
        result = validator.validate(node())
        assert not result.passes


def test_complex_pydantic_plugin_check_output_passes():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    def dummy() -> Repo:
        return Repo(
            name="hamilton",
            owner=Owner(name="DAGWorks-Inc"),
            versions=[Version(name="0.1.0", id=1), Version(name="0.2.0", id=2)],
        )

    node = Node.from_fn(dummy)
    validators = h_pydantic.check_output().get_validators(node)
    assert len(validators) == 1
    validator = validators[0]
    result_success = validator.validate(node())
    assert result_success.passes


def test_complex_pydantic_plugin_check_output_fails():
    class Owner(BaseModel):
        name: str

    class Version(BaseModel):
        name: str
        id: int

    class Repo(BaseModel):
        name: str
        owner: Owner
        versions: list[Version]

    def dummy() -> Repo:
        return Repo(
            name="hamilton",
            owner=Owner(name="DAGWorks-Inc"),
            versions=[
                Version(name="0.1.0", id=1),
                Version(name="0.2.0", id="two"),  # type: ignore
            ],
        )

    node = Node.from_fn(dummy)
    validators = h_pydantic.check_output().get_validators(node)
    assert len(validators) == 1
    validator = validators[0]

    with pytest.raises(ValidationError):
        result = validator.validate(node())
        assert not result.passes
