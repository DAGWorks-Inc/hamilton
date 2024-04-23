import dataclasses


@dataclasses.dataclass
class GitInfo:
    branch: str
    commit_hash: str
    committed: bool
    repository: str
    local_repo_base_path: str


@dataclasses.dataclass
class Project:
    project_name: str
    owner: str
