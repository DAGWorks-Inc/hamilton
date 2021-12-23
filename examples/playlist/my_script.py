import importlib
import logging
import sys

import pandas as pd
from hamilton import driver

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout)
# you could control loading the data
# raw_data = pd.read_csv('movies.csv')
# initial_input = {
#     'movie_title': raw_data['movie_title'],
#     'title_year': raw_data['title_year'],
#     'genres': raw_data['genres'],
#     'gross': raw_data['gross'],
#     'genre': 'Sci-Fi'
# }
# or you could have a function do it for you
initial_input = {
    'movie_csv_file_name': 'movies.csv',
    'genre': 'Sci-Fi'
}
# we need to tell hamilton where to load function definitions from
module_name = 'playlist_101'
module = importlib.import_module(module_name)
dr = driver.Driver(initial_input, module)  # can pass in multiple modules
# we need to specify what we want in the final dataframe.
output_columns = [
    'movie_title',
    'genre_match',
    'bonus_movie',
]
# let's create the dataframe!
df = dr.execute(output_columns, display_graph=True)
# now we can filter/save/display as we like
df = df[df.genre_match]
print(df.head(5))
