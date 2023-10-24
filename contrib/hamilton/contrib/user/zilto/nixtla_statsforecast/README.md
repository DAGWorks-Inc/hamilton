# Purpose of this module

This module implements forecasting using statistical methods with `statsforecast` by Nixtla.

Fit and evaluate a list of models on a time series dataset. Obtain a cross-validation benchmark dataframe and prediction plots.

Your dataset needs to have columns `unique_id` to identify each series, `ds` to identify the time step, and `y` to specify the value of series `unique_id` at time `ds`.

# Configuration Options
## Config.when
This module doesn't receive configurations.

## Inputs
- `freq`: Adjust to meet the sampling rate of your time series
- `cv_forecast_steps`, `cv_window_size`, `n_cv_windows`: Change these values to define your cross-validation strategy.
- ⚠ `n_jobs`: Set the number of cores to use for compute. The default value `-1` will use all available cores and might slowdown the machine in the meantime.

## Overrides
- `base_models`: Set the list of Nixtla models to fit and evaluate ([docs](https://nixtla.github.io/statsforecast/src/core/models.html))
- `evaluation_metrics`: Set the list of Nixtla-compatible metrics to use during cross-validation ([examples](https://github.com/Nixtla/utilsforecast/blob/main/utilsforecast/losses.py))


# Limitations
- This flow doesn't include dataset preprocessing steps.
