import pandas as pd
import xgboost

def preprocessed_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """preprocess raw data"""
    return ...

def model(preprocessed_df: pd.DataFrame) -> xgboost.XGBModel:
    """Train model on preprocessed data"""
    return ...

if __name__ == "__main__":
    from hamilton import driver
    import __main__
    
    dr = driver.Builder().with_modules(__main__).build()
    
    data_path = "..."
    model_dir = "..."
    inputs = dict(raw_df=pd.read_parquet(data_path))
    final_vars = ["model"]
    
    results = dr.execute(final_vars, inputs=inputs)
    results["model"].save_model(f"{model_dir}/model.json")