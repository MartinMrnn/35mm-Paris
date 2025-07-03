"""
Script to import movies from Allocine API.
Modern version with proper logging and error handling.
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add backend/src to import path
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.insert_logic import bulk_insert_movies
from utils.logger import get_logger
from allocineAPI.allocineAPI import allocineAPI

logger = get_logger(__name__)


def fetch_movies(cinema_id: str, date: Optional[str] = None):
    """
    Fetch movies from Allocine API.
    
    Args:
        cinema_id: Cinema ID (e.g., "P3757")
        date: Date in YYYY-MM-DD format, defaults to today
        
    Returns:
        List of movie data
    """
    if not date:
        date = datetime.today().strftime("%Y-%m-%d")
    
    api = allocineAPI()
    
    try:
        logger.info("Fetching movies from Allocine", 
                   cinema_id=cinema_id, 
                   date=date)
        movies = api.get_movies(cinema_id, date)
        logger.info("Movies fetched successfully", count=len(movies))
        return movies
    except Exception as e:
        logger.error("Failed to fetch movies", 
                    cinema_id=cinema_id,
                    date=date,
                    error=str(e))
        return []


def main():
    """Main entry point."""
    logger.info("Starting movie import process")
    
    # Configuration
    cinema_id = "P3757"  # UGC Gobelins, Paris
    date = datetime.today().strftime("%Y-%m-%d")
    
    # Fetch movies
    api = allocineAPI()
    try:
        movies = api.get_movies(cinema_id, date)
        logger.info(f"Fetched {len(movies)} movies")
    except Exception as e:
        logger.error(f"Failed to fetch movies: {e}")
        return
    
    # Import movies
    inserted, skipped = bulk_insert_movies(movies)
    
    # Summary
    logger.info(f"Import complete: {inserted} inserted, {skipped} skipped")


if __name__ == "__main__":
    main()