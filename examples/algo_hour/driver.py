from algo_hour import data_loaders, transform_data
from hamilton import driver

# Initialize the driver with our modules
# Config is empty for now...
dr = driver.Driver({}, data_loaders, transform_data)

output_columns = [
    'spend',
    'signups',
    'avg_3wk_spend',
    'spend_per_signup',
    'spend_zero_mean_unit_variance'
]
# let's create the dataframe!
df = dr.execute(
    output_columns,
    inputs={
        'demand_db': 'sf_hamilton_algo_hour_example',
        'spend_table': 'spend',
        'signups_table': 'signups'})
print(df.to_string())

# To visualize do `pip install sf-hamilton[visualization]` if you want these to work
dr.visualize_execution(
    output_columns,
    './my_dag.dot',
    render_kwargs={},
    inputs={
        'demand_db': 'sf_hamilton_algo_hour_example',
        'spend_table': 'spend',
        'signups_table': 'signups'})
dr.display_all_functions('./my_full_dag.dot')
