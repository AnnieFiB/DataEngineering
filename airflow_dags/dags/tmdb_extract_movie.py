import os
import requests
import pandas as pd
from datetime import datetime
from utils import get_file_path, load_environment_variables  

# Load environment variables
load_environment_variables()  # Load variables from .env file

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_AUTH_TOKEN = os.getenv("TMDB_API_READ_ACCESS_TOKEN_AUTH")
TMDB_BASE_URL = os.getenv("TMDB_BASE_URL")
AUTH = os.getenv("TMDB_AUTHENTICATION_URL")


headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_AUTH_TOKEN}"
}

def extract_movie_data(start, end):
    """
    Extracts movie data from TMDB API and appends it to allmovies.csv,
    avoiding duplicates and tagging each row with batch_id and timestamp.
    """
    all_movies = []

    print(f"Extracting movie data from page {start} to {end}...")

    for n in range(start, end + 1):
        print(f"Fetching page {n} of popular movies...")
        url = f"https://api.themoviedb.org/3/discover/movie?page={n}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json().get('results', [])
            all_movies.extend(data)
        else:
            print(f" Failed to fetch page {n}: {response.status_code}")
            response.raise_for_status()

    print(f"\n Total movies extracted: {len(all_movies)}")

    df_new = pd.DataFrame()  # default
    df_combined = pd.DataFrame()

    if all_movies:
        df_new = pd.DataFrame(all_movies)
        df_new['batch_id'] = f"batch_{start}_{end}"
        df_new['timestamp'] = datetime.now().isoformat()

        file_path = get_file_path("allmovies.csv")

        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['id'])
        else:
            df_combined = df_new

        df_combined.to_csv(file_path, index=False)
        print(f"\n Movie data updated in {file_path} (total: {len(df_combined)} records)")

    return df_new, df_combined
