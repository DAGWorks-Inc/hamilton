# Parametrizing the DAG

Static DAGs are only so useful. In the real world, we need to be able to configure both the shape of the DAG and the inputs to the DAG as part of the driver. The default driver comes with three input types that you can control. They both take the form `Dict[str, Any]`

1. **config** The config is a dictionary of strings to values. This is passed into the constructor of the driver, as it is required to create the DAG. It _also_ get's passed into the DAG at runtime, so you have access to parameter values. See [decorators](decorators.md) for how the config can be used.&#x20;
2. **inputs **_****_ The runtime\_inputs to the DAG. These have to be mutually disjoint from the config -- overrides don't make sense here, as the DAG has been constructed assuming fixed configs.
3. **overrides** Values to override nodes in a DAG. During execution, nothing upstream of these are computed.

Let's go through some examples...

**You have a DAG for region and business line**, where the rolling average for marketing spend is computed differently (see [quickstart](../quick-start/) for the motivating example). In this case, you'll define the DAG as follows:

```python
@config.when(business_line='CA')
def avg_rolling_spend__CA(spend: pd.Series) -> pd.Series:
    """Rolling average of spend in the canada region."""
    return spend.rolling(3).mean()

@config.when(business_line='US')
def avg_rolling_spend__US(spend: pd.Series) -> pd.Series:
    """Rolling average of spend in the US region."""
    return spend.rolling(2).mean()
```

When the graph is compiled, the implementation of `avg_rolling_spend` varies based off of the configuration value. You would construct the driver with `config={'region' : 'US'}` , to get the desired behavior.

**You want to pass in the region/business line to change the behavior or a transform.** Say you have a big dataframe of marketing spend with columns representing the region, and want to filter it out for the individual region. You would define the transform as follows.&#x20;

```python
def avg_rolling_spend(spend_by_country: pd.DataFrame, region: str) -> pd.Series:
    """Rolling average of spend in the specified region."""
    return spend_by_country[spend_by_country.region==region].spend
```

You would execute the driver with `input={'region' : 'US'}`, to get the desired behavior. You could _also_ construct the DAG with `input={'region' : 'US'}.`

**You want to override the value of a transform**. In this case, you can just pass this into the execute function of the driver as overrides. E.G.:

```python
df = driver.execute(
    ['acquisition_cost'], 
    overrides={'spend' : pd.Series(
        [40, 80, 100, 400, 800, 1000], # what if we increased the marketing spend?
        index=pd.date_range("2022-01-01", periods=6, freq="w"))})
```
