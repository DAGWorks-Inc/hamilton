import polars as pl
def create_lf() -> pl.LazyFrame:
    raw_data = {
        'fatf_increased_monitoring_grey_list': ['Yes', 'No', 'Yes', None],
        'fatf_call_for_action_black_list': ['No', 'Yes', None, 'Yes'],
        'overall_score': [3.0, 4.5, 5.0, 1.0],
        'iso_code': ['USA', 'CAN', 'MEX', 'RUS']
    }
    raw_df = pl.LazyFrame(raw_data, schema={"fatf_increased_monitoring_grey_list": pl.String,
                                            "fatf_call_for_action_black_list": pl.String,
                                            "overall_score": pl.Float32,
                                            "iso_code": pl.String})
    return raw_df
def do_something(create_lf: pl.LazyFrame) -> pl.LazyFrame:
    return create_lf
