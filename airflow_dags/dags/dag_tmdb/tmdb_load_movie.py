import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv, find_dotenv
import os

Base = declarative_base()

# Load environment variables
load_dotenv(find_dotenv())

# Postgres Connection URL (either dynamically retrieved or from the environment variables)
def get_postgres_conn():
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


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
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print(f"\n Loading {len(df)} rows to table '{table_name}'...")
    
    # Iterate over the DataFrame and create insert queries
    for _, row in df.iterrows():
        insert_query = text("""
            INSERT INTO movies (id, original_title, overview, popularity, release_date, rating, vote_count, batch_id, timestamp)
            VALUES (:id, :original_title, :overview, :popularity, :release_date, :rating, :vote_count, :batch_id, :timestamp)
            ON CONFLICT (id) DO UPDATE
            SET original_title = EXCLUDED.original_title,
                overview = EXCLUDED.overview,
                popularity = EXCLUDED.popularity,
                release_date = EXCLUDED.release_date,
                rating = EXCLUDED.rating,
                vote_count = EXCLUDED.vote_count,
                batch_id = EXCLUDED.batch_id,
                timestamp = EXCLUDED.timestamp;
        """)
        
        # Execute the query
        session.execute(insert_query, {
            'id': int(row["id"]),
            'original_title': str(row["original_title"]),
            'overview': str(row["overview"]),
            'popularity': float(row["popularity"]),
            'release_date': row["release_date"],
            'rating': float(row["rating"]),
            'vote_count': int(row["vote_count"]),
            'batch_id': str(row["batch_id"]),
            'timestamp': row["timestamp"]
        })

    # Commit the transaction
    session.commit()
    session.close()
    print(f"\n Loaded {len(df)} rows to table '{table_name}'")