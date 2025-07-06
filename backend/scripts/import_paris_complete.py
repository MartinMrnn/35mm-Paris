"""
Script to import all Paris cinema data.
Imports cinemas, movies and screenings for all Paris.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

# Add backend/src to import path
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.insert_logic import (
    process_cinema_screenings, 
    insert_cinema_from_location,
    insert_cinema
)
from utils.logger import get_logger
from allocineAPI.allocineAPI import allocineAPI

logger = get_logger(__name__)


# Paris location ID
PARIS_VILLE_ID = "ville-75056"

# Or use specific arrondissements/departments
PARIS_LOCATIONS = [
    "ville-75056",  # Paris (all)
    # Add more if needed
]


def import_paris_cinemas() -> List[str]:
    """
    Import all Paris cinemas and return their IDs.
    
    Returns:
        List of cinema IDs
    """
    logger.info("Importing Paris cinemas")
    
    all_cinema_ids = []
    
    for location_id in PARIS_LOCATIONS:
        cinema_ids = insert_cinema_from_location(location_id)
        all_cinema_ids.extend(cinema_ids)
    
    # Remove duplicates
    all_cinema_ids = list(set(all_cinema_ids))
    
    logger.info(f"Found {len(all_cinema_ids)} unique cinemas in Paris")
    return all_cinema_ids


def import_screenings_for_cinemas(cinema_ids: List[str], days_ahead: int = 7):
    """
    Import screenings for given cinemas.
    
    Args:
        cinema_ids: List of cinema IDs
        days_ahead: Number of days to import
    """
    # AJOUT : Récupérer et insérer les cinémas depuis l'API Paris
    api = allocineAPI()
    
    try:
        logger.info("Fetching all Paris cinemas from API")
        all_paris_cinemas = api.get_cinema("ville-115755")  # Paris
        
        # Filtrer pour ne garder que les cinémas demandés
        target_cinemas = [c for c in all_paris_cinemas if c["id"] in cinema_ids]
        
        logger.info(f"Found {len(target_cinemas)} target cinemas out of {len(all_paris_cinemas)} total Paris cinemas")
        
        # Insérer chaque cinéma ciblé
        for cinema_data in target_cinemas:
            logger.info(f"Inserting cinema {cinema_data['id']}: {cinema_data['name']}")
            # Ajouter city et zipcode par défaut pour Paris
            cinema_data["city"] = "Paris"
            cinema_data["zipcode"] = "75000"  # Default, will be updated if needed
            insert_cinema(cinema_data)
            
    except Exception as e:
        logger.error(f"Failed to fetch Paris cinemas from API: {e}")
        return 0, 0
    
    # ...existing code...
    total_movies = 0
    total_screenings = 0
    
    # Generate dates
    dates = []
    for i in range(days_ahead):
        date = (datetime.today() + timedelta(days=i)).strftime("%Y-%m-%d")
        dates.append(date)
    
    logger.info(f"Importing {len(dates)} days for {len(cinema_ids)} cinemas")
    
    # Process each cinema and date
    for i, cinema_id in enumerate(cinema_ids):
        logger.info(f"Processing cinema {i+1}/{len(cinema_ids)}: {cinema_id}")
        
        for date in dates:
            movies, screenings = process_cinema_screenings(cinema_id, date)
            total_movies += movies
            total_screenings += screenings
            
            # Small delay to avoid hitting API limits
            import time
            time.sleep(0.5)
    
    logger.info("Import complete",
               total_movies=total_movies,
               total_screenings=total_screenings)
    
    return total_movies, total_screenings


def import_single_cinema_test():
    """Test import with a single known cinema."""
    logger.info("Running single cinema test")
    
    # UGC Gobelins - we know this one works
    cinema_id = "P3757"
    
    # First, insert the cinema manually since get_cinema needs location_id
    insert_cinema({
        "id": cinema_id,
        "name": "UGC Gobelins",
        "address": "66 avenue des Gobelins",
        "city": "Paris",
        "zipcode": "75013"
    })
    
    # Import today's screenings
    date = datetime.today().strftime("%Y-%m-%d")
    movies, screenings = process_cinema_screenings(cinema_id, date)
    
    logger.info(f"Test complete: {movies} movies, {screenings} screenings")


def main():
    """Main entry point."""
    
    # Option 1: Test with single cinema first
    #import_single_cinema_test()
    
    # Option 2: Import a few days for known cinemas
    known_cinema_ids = ["P3757", "C0159", "C0013"]  # Add more as needed
    import_screenings_for_cinemas(known_cinema_ids, days_ahead=3)
    
    # Option 3: Full Paris import (uncomment when ready)
    # cinema_ids = import_paris_cinemas()
    # import_screenings_for_cinemas(cinema_ids[:5], days_ahead=1)  # Start small


if __name__ == "__main__":
    main()