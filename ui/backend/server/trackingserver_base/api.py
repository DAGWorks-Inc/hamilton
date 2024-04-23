from ninja import Router, Schema
from ninja.errors import HttpError
from trackingserver_base.shared_types.attributes import (
    Attribute__dagworks_describe__3,
    Attribute__dict__1,
    Attribute__dict__2,
    Attribute__documentation_loom__1,
    Attribute__error__1,
    Attribute__pandas_describe__1,
    Attribute__primitive__1,
    Attribute__unsupported__1,
)
from trackingserver_base.shared_types.code_version import CodeVersion__git__1
from trackingserver_base.shared_types.node_metadata import NodeMetadata__python_type__1

router = Router(tags=["auth"])


class AllAttributeTypes(Schema):
    documentation_loom__1: Attribute__documentation_loom__1
    primitive__1: Attribute__primitive__1
    unsupported__1: Attribute__unsupported__1
    pandas_describe__1: Attribute__pandas_describe__1
    error__1: Attribute__error__1
    dict__1: Attribute__dict__1
    dict__2: Attribute__dict__2
    dagworks_describe__3: Attribute__dagworks_describe__3


class AllCodeVersionTypes(Schema):
    git__1: CodeVersion__git__1


class AllNodeMetadataTypes(Schema):
    python_type__1: NodeMetadata__python_type__1


@router.get(
    "/v1/metadata/attributes/schema", response=AllAttributeTypes, tags=["metadata", "attributes"]
)
async def get_attributes_type(
    request,
) -> AllAttributeTypes:
    error = HttpError(status_code=400, message="This only exists to populate the openAPI schema.")
    raise error


@router.get(
    "/v1/metadata/code_versions/schema",
    response=AllCodeVersionTypes,
    tags=["metadata", "attributes"],
)
async def get_code_version_types(
    request,
) -> AllCodeVersionTypes:
    error = HttpError(status_code=400, message="This only exists to populate the openAPI schema.")
    raise error


@router.get(
    "/v1/metadata/node_metadata/schema",
    response=AllNodeMetadataTypes,
    tags=["metadata", "attributes"],
)
async def get_node_metadata_types(
    request,
) -> AllNodeMetadataTypes:
    error = HttpError(status_code=400, message="This only exists to populate the openAPI schema.")
    raise error
