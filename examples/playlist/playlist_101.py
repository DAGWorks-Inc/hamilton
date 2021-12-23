import pandas as pd

from hamilton.function_modifiers import extract_columns


@extract_columns('movie_title', 'title_year', 'genres', 'gross')
def load_movies_csv(movie_csv_file_name: str) -> pd.DataFrame:
    return pd.read_csv(movie_csv_file_name)


def genre_match(genres: pd.Series, genre: str) -> pd.Series:
    """Returns an indicator for movies that match the desired genre."""
    def match(movie_genres: str) -> bool:
        return genre.lower() in movie_genres.lower()
    return genres.apply(match)


def bonus_movie(movie_title: pd.Series, genre_match: pd.Series) -> str:
    """Chooses a movie at random that does not match the genre."""
    # Choose one randomly.
    return movie_title[genre_match == False].sample().iloc[0]
