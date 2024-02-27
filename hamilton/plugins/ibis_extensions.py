from hamilton import registry

try:
    import ibis.expr.types as ir
except ImportError:
    raise NotImplementedError("Ibis is not installed.")


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
