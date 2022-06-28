import enum
import typing
from typing import Optional, Type

import libcst as cst
import pandas as pd


def get_subscript_name(subscript: cst.Subscript) -> str:
    prefix = subscript.value.value
    slice_ = subscript.slice
    if len(slice_) == 0:
        raise_unsupported("Invalid target for assignment. Only single assignments are allowed, E.G. df['a'] = ...", assign_target)
    subscript, *_ = slice_
    value = subscript.slice.value
    if not isinstance(value, cst.SimpleString):
        raise_unsupported("Invalid target for assignment. Only single assignments are allowed, E.G. df['a'] = ...", assign_target)
    return f'{prefix}_{value.raw_value}'  # this is a quick hack to make this work


def get_name_name(name: cst.Name) -> str:
    return name.value


def raise_unsupported(reason: str, node: cst.CSTNode):
    raise ValueError(f"The following code is not supported: {to_code(node)}. {reason}")


def to_code(node: cst.CSTNode) -> str:
    return cst.Module([]).code_for_node(node)


# class DependentsCollector(cst.CSTVisitor):
#     def __init__(self):
#         self.dependents = []
#
#     # def visit_Name(self, node: cst.Name) -> bool:
#     #     self.dependents.append(get_name_name(node))
#     #     print(node)
#     #     return False
#
#     def visit_Expr(self, node: "Expr") -> Optional[bool]:
#         pass
#
#     def visit_Subscript(self, node: "Subscript") -> Optional[bool]:
#         self.dependents.append(get_subscript_name(node))
#         return False
#
#     def visit_Call_func(self, node: "Call") -> None:
#         import pdb
#         pdb.set_trace()
#         return False
#
#     # TODO -- add more here to find
class SupportedType(enum.Enum):
    DATAFRAME = 'pd.DataFrame'
    SERIES = 'pd.Series'
    INTEGER = 'int'
    FLOAT = 'float'
    STRING = 'string'
    ANY = 'typing.Any'

    # TODO -- add more

    @staticmethod
    def from_annotation(annotation: cst.Annotation) -> 'SupportedType':
        if annotation is None:
            return SupportedType.ANY
        if isinstance(annotation.annotation, cst.Attribute):
            value = annotation.annotation.value
            attr = annotation.annotation.attr
            if value.value == 'pd':
                if attr.value == 'DataFrame':  # This is a little sloppy
                    return SupportedType.DATAFRAME
                if attr.value == 'Series':
                    return SupportedType.SERIES
                raise ValueError(f"Unsupported pandas type annotation: {attr.value}")
        return SupportedType(annotation.value.value)  # Just assuming its a name, let's see how this works

    @staticmethod
    def from_type(type: Type[Type]) -> 'SupportedType':
        if type == pd.Series:
            return SupportedType.SERIES
        elif type == pd.DataFrame:
            return SupportedType.DATAFRAME
        elif type == typing.Any:
            return SupportedType(type.__name__)
        else:
            return SupportedType(type.__name__)

    def to_annotation(self):
        return cst.parse_module(self.value).body[0].body[0]

    @staticmethod
    def subscript(type_: 'SupportedType'):
        if type_ == SupportedType.DATAFRAME:
            return SupportedType.SERIES
        raise ValueError(f"Trying to subscript: {type_}, not allowed")


def derive_types_from_dependent_types(dependent_types: typing.List[SupportedType]):
    if len(set(dependent_types)) == 1:
        return dependent_types[0]
    return SupportedType.ANY # Just a quick heuristic/guess, won't always be right