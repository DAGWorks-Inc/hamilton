import base64
import zlib
from typing import Dict, Mapping


def _compress_string(string: str) -> str:
    return base64.b64encode(zlib.compress(string.encode(), level=3)).decode()


def _decompress_string(string: str) -> str:
    return zlib.decompress(base64.b64decode(string.encode())).decode()


def _encode_str_dict(d: Mapping) -> str:
    interleaved_tuple = tuple(item for pair in sorted(d.items()) for item in pair)
    return ",".join(interleaved_tuple)


def _decode_str_dict(s: str) -> Mapping:
    interleaved_tuple = tuple(s.split(","))
    d = {}
    for i in range(0, len(interleaved_tuple), 2):
        d[interleaved_tuple[i]] = interleaved_tuple[i + 1]
    return d


def decode_key(cache_key: str) -> tuple:
    code_and_data_string = _decompress_string(cache_key)
    code_version, _, data_stringified = code_and_data_string.partition(",")
    if data_stringified == "<none>":
        dep_data_versions = {}
    else:
        dep_data_versions = _decode_str_dict(data_stringified)
    return code_version, dep_data_versions


def create_cache_key(code_version: str, dep_data_versions: Dict[str, str]) -> str:
    if len(dep_data_versions.keys()) > 0:
        dependencies_stringified = _encode_str_dict(dep_data_versions)
    else:
        dependencies_stringified = "<none>"

    return _compress_string(f"{code_version},{dependencies_stringified}")
