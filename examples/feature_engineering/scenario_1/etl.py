"""
This is part of an offline ETL that you'd likely have.
You pull data from a source, and transform it into features, and then save/fit a model with them.

Here we ONLY use Hamilton to create the features, with comment stubs for the rest of the ETL that would normally
be here.
"""
import features
import named_model_feature_sets
import offline_loader
import pandas as pd

from hamilton import driver


def create_features(source_location: str) -> pd.DataFrame:
    """Extracts and transforms data to create feature set.

    Hamilton functions encode:
     - pulling the data
     - transforming the data into features

    Hamilton then handles building a dataframe.

    :param source_location: the location to load data from.
    :return: a pandas dataframe.
    """
    model_features = named_model_feature_sets.model_x_features
    config = {}
    dr = driver.Driver(config, offline_loader, features)
    # Visualize the DAG if you need to:
    # dr.display_all_functions('./offline_my_full_dag.dot', {"format": "png"})
    dr.visualize_execution(
        model_features,
        "./offline_execution.dot",
        {"format": "png"},
        inputs={"location": source_location},
    )
    df = dr.execute(
        # add age_mean and age_std_dev to the features
        model_features + ["age_mean", "age_std_dev"],
        inputs={"location": source_location},
    )
    return df


if __name__ == "__main__":
    # stick in command line args here
    _source_location = "../../data_quality/pandera/Absenteeism_at_work.csv"
    _features_df = create_features(_source_location)
    # we need to store `age_mean` and `age_std_dev` somewhere for the online side.
    # exercise for the reader: where would you store them for your context?
    # ideas: with the model? in a database? in a file? in a feature store? (all reasonable answers it just
    # depends on your context).
    _age_mean = _features_df["age_mean"].values[0]
    _age_std_dev = _features_df["age_std_dev"].values[0]
    print(_features_df)
    # Then do something with the features_df, e.g. define functions to do the following:
    #   save_features(features_df[named_model_feature_sets.model_x_features], "my_model_features.csv")
    #   train_model(features_df[named_model_feature_sets.model_x_features])
    #   etc.
