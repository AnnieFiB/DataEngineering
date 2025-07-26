import pandas as pd
from datetime import datetime
from utils import get_file_path

# Function to transform movie data
def transform_movies(df_combined):
    df = df_combined.copy()
    df = df[["id", "original_title", "overview", "popularity", "release_date",
             "vote_average", "vote_count", "batch_id", "timestamp"]]

    df.rename(columns={"vote_average": "rating"}, inplace=True)

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_date"] = df["release_date"].fillna(datetime(1900, 1, 1))

    file_path = get_file_path("CleanedMovies.csv")
    df.to_csv(file_path, index=False)
    
    return df
