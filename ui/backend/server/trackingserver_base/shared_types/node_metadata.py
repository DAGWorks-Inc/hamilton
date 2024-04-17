from pydantic import BaseModel


class NodeMetadata:
    _version: int


class NodeMetadata__python_type__1(BaseModel, NodeMetadata):
    _version: int = 1
    type_name: str
