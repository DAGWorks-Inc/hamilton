import pyarrow as pa
import pyspark.sql.types as pt
import pytest

from hamilton.plugins import h_schema


@pytest.mark.parametrize(
    "spark_type,arrow_type",
    [
        (pt.NullType(), pa.null()),
        (pt.BooleanType(), pa.bool_()),
        (pt.BinaryType(), pa.binary()),
        (pt.ByteType(), pa.int8()),
        (pt.ShortType(), pa.int16()),
        (pt.IntegerType(), pa.int32()),
        (pt.LongType(), pa.int64()),
        (pt.DateType(), pa.date64()),
        (pt.FloatType(), pa.float32()),
        (pt.DoubleType(), pa.float64()),
        (pt.TimestampType(), pa.timestamp(unit="ms", tz=None)),
        (pt.TimestampNTZType(), pa.timestamp(unit="ms", tz=None)),
        (pt.StringType(), pa.string()),
        (pt.VarcharType(length=10), pa.string()),
        (pt.CharType(length=10), pa.string()),
        (pt.DayTimeIntervalType(), pa.month_day_nano_interval()),
        (pt.YearMonthIntervalType(), pa.month_day_nano_interval()),
        (pt.DecimalType(precision=5, scale=2), pa.decimal128(precision=5, scale=2)),
        (pt.ArrayType(pt.FloatType()), pa.array([], type=pa.float32())),
        (pt.MapType(pt.ByteType(), pt.StringType()), pa.map_(pa.int8(), pa.string())),
        (
            pt.StructType(
                fields=[pt.StructField("a", pt.FloatType()), pt.StructField("b", pt.StringType())]
            ),
            pa.struct(fields=[pa.field("a", pa.float32()), pa.field("b", pa.string())]),
        ),
    ],
)
def test_spark_to_arrow_type(spark_type, arrow_type):
    converted_type = h_schema._spark_to_arrow(spark_type)
    assert converted_type == arrow_type
