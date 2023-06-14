from datetime import datetime

from airflow.decorators import dag, task


@task
def load_data():
    return


@task
def preprocess_features():
    return


@task
def train_and_evaluate_model():
    return


@task
def store_model():
    return


@dag(
    dag_id="airflow-hamilton-demo",
    description="Predict absenteism using linear regression",
    start_date=datetime(2023, 1, 1),
)
def absenteeism_prediction_dag():
    (load_data() >> preprocess_features() >> train_and_evaluate_model() >> store_model())


absenteeism_prediction = absenteeism_prediction_dag()
