import pandas as pd

# Columns are a,b,c, and d

df: pd.DataFrame = pd.read_csv('test_data')
df_2: pd.DataFrame = pd.read_csv('test_data_2')
def _do_something_with_two_series(a: pd.Series, b: pd.Series) -> pd.Series:
    return a ** b


df['e'] = df['a'] + df['b'] + df['c'] + df['d']
df['a'] = df['a'] * 2
df['b'] = df['c'] + \
          df['b'] + df['d']
df['f'] = df['a'] * df['b'] * \
          df['c'] * df['d'] * df['e']
df_2['a'] = df_2['b'] + df_2['c']
df['g'] = _do_something_with_two_series(df['f'], df['e'])
