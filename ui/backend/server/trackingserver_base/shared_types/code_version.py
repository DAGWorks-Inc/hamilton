from pydantic import BaseModel


class CodeVersion:
    _version: int


class CodeVersion__git__1(BaseModel, CodeVersion):
    _version: int = 1
    git_hash: str
    git_repo: str
    git_branch: str
    committed: bool
