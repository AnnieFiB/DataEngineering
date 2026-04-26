import requests
import os 
from dotenv import load_dotenv
import csv
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine,  Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

# Load environment variables from .env file
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_AUTH_TOKEN = os.getenv("TMDB_API_READ_ACCESS_TOKEN_AUTH")
TMDB_BASE_URL = os.getenv("TMDB_BASE_URL")
AUTH = os.getenv("TMDB_AUTHENTICATION_URL")     
db_url = os.getenv("BASE_URL")  


headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_AUTH_TOKEN}"
}


# === 1. Extract Movie Data ===
import os
import requests
import pandas as pd
from datetime import datetime

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
            print(f"❌ Failed to fetch page {n}: {response.status_code}")
            response.raise_for_status()

    print(f"\n Total movies extracted: {len(all_movies)}")

    df_new = pd.DataFrame()  # default
    df_combined = pd.DataFrame()

    if all_movies:
        df_new = pd.DataFrame(all_movies)
        df_new['batch_id'] = f"batch_{start}_{end}"
        df_new['timestamp'] = datetime.now().isoformat()

        file_path = "allmovies.csv"

        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['id'])
        else:
            df_combined = df_new

        df_combined.to_csv(file_path, index=False)
        print(f"\n Movie data updated in {file_path} (total: {len(df_combined)} records)")

    return df_new, df_combined



# === Transformation Function ===

def transform_movies(df_combined):
    df = df_combined.copy()
    df = df[["id", "original_title", "overview", "popularity", "release_date",
             "vote_average", "vote_count", "batch_id", "timestamp"]]

    df.rename(columns={"vote_average": "rating"}, inplace=True)

    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_date"] = df["release_date"].fillna(datetime(1900, 1, 1))
     
    df.to_csv("CleanedMovies.csv", index=False)
    
   # bad_rows = df[df["release_date"].astype(str).str.contains("NaT")]
    #print("🧨 Problem rows:")
    #print(bad_rows)

    return df


# === Load to DB ===

class Movie(Base):
    __tablename__ = "movies"

    movie_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer, unique=True, nullable=False)   
    original_title = Column(String, nullable=False)
    overview = Column(String, nullable=True)
    popularity = Column(Float, nullable=True)
    release_date = Column(DateTime, nullable=True)
    rating = Column(Float, nullable=True)
    vote_count = Column(Integer, nullable=True)
    batch_id = Column(String, nullable=True)
    timestamp = Column(DateTime, nullable=True)

def load_to_db(df, db_url, table_name="movies"):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    print(f"\n Loading {len(df)} rows to table '{table_name}'...")
    
    for _, row in df.iterrows():
            movie = Movie(
                id=int(row["id"]),
                original_title=str(row["original_title"]),
                overview=str(row["overview"]),
                popularity=float(row["popularity"]),
                release_date=row["release_date"],
                rating=float(row["rating"]),
                vote_count=int(row["vote_count"]),
                batch_id=str(row["batch_id"]),
                timestamp=row["timestamp"]
            )
            session.add(movie)
    session.commit()
    session.close()
    print(f"\n Loaded {len(df)} rows to table '{table_name}'")
    
    
    
# === Main ETL Function ===

def extract_transform_load(start, end, db_url=None, table_name=None):
    """
    Main ETL function to extract movie data, transform it, and load it into a database.
    """
    df_new, df_combined = extract_movie_data(start, end)

    if not df_combined.empty:
        df_transformed = transform_movies(df_combined)
        
        if db_url and table_name:
            load_to_db(df_transformed, db_url, table_name)
        else:
            print("\n No database URL or table name provided. Skipping load step.")
    else:
        print("\n No movie data extracted. ETL process terminated.")


extract_transform_load(203, 204, db_url, table_name="movies")





# extract_movie_data(100, 102)






'''
def extract_movie_data1(start, end):
    """
    Extracts movie data from TMDB API.
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
            print(f"Failed to fetch page {n}: {response.status_code}")
            response.raise_for_status()

    if all_movies:
        print(f"Total movies extracted: {len(all_movies)}")
        file = os.path.isfile("movies.csv")
        write_header = not file or os.stat("movies.csv").st_size == 0
        
        with open("movies.csv", "a", newline="", encoding="utf-8") as f:
           writer = csv.DictWriter(f, fieldnames=all_movies[0].keys())
           if write_header:
               writer.writeheader()
               writer.writerows(all_movies)
        print("Movie data saved to movies.csv")


'''
