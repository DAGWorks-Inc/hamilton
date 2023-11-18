# Define your new Hamilton functions.
# The %%writefile magic command creates a new Python module with the functions below.
# We will import this later and pass it into our Driver.

import geopandas
import geodatasets
from hamilton.function_modifiers import extract_columns

@extract_columns("community", "POP2010", "geometry")
def base_df(base_df_location: str) -> geopandas.GeoDataFrame:
    """Loads base dataframe of data.

    :param base_df_location: just showing that we could load this from a file...
    :return:
    """
    chicago = geopandas.read_file(geodatasets.get_path("geoda.chicago_commpop"))
    return chicago
 
# Look at `my_functions` to see how these functions connect.
def chicago_area(geometry: geopandas.GeoSeries) -> geopandas.GeoSeries:
    """Get the area of the row using the geometry column"""
    return 10000 * geometry.area 

def chicago_population(POP2010: geopandas.GeoSeries) -> geopandas.GeoSeries:
    """ Get the population of the area of interest"""
    return POP2010

def chicago_population_density(chicago_area: geopandas.GeoSeries, chicago_population: geopandas.GeoSeries) -> geopandas.GeoSeries:
    """ Calculate the population density"""
    return chicago_population/chicago_area
