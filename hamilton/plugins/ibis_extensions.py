from typing import Type

from hamilton import registry

try:
    import ibis
    import ibis.expr.types as ir
except ImportError:
    raise NotImplementedError("Ibis is not installed.")

from hamilton.data_quality import base, default_validators

DATAFRAME_TYPE = ir.Table
COLUMN_TYPE = ir.Column


def view_expression(expression: ir.Expr, **kwargs):
    import ibis.expr.visualize as viz

    dot = viz.to_graph(expression)
    dot.render(**kwargs)
    return dot


@registry.get_column.register(ir.Table)
def get_column_ibis(df: ir.Table, column_name: str) -> ir.Column:
    return df[column_name]


def register_types():
    """Function to register the types for this extension."""
    registry.register_types("ibis", DATAFRAME_TYPE, COLUMN_TYPE)


register_types()


class SchemaValidatorIbis(base.DataValidator):
    def __init__(self, schema: ibis.expr.schema.Schema, importance: str):
        """
        `schema` is an ordered mapping.
        """
        super(SchemaValidatorIbis, self).__init__(importance)
        self.schema = schema

    def name(self) -> str:
        return f"{self.arg()}_validator"

    @classmethod
    def arg(cls) -> str:
        return "schema"

    @classmethod
    def applies_to(cls, datatype: Type[Type]) -> bool:
        return issubclass(datatype, ir.Table)

    def description(self) -> str:
        return "Validates that ibis Table expression matches schema."

    def validate(self, data: ir.Table) -> base.ValidationResult:
        passes = data.schema().equals(self.schema)
        message = ""
        diagnostics = {}
        return base.ValidationResult(
            passes=passes,
            message=message,
            diagnostics=diagnostics,
        )


def register_validators():
    default_validators.AVAILABLE_DEFAULT_VALIDATORS.append(SchemaValidatorIbis)


register_validators()
