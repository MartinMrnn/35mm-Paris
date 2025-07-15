"""
Database insertion logic for 35mm Paris.
Modern, type-safe, with proper error handling.
"""
import re
import hashlib
from typing import Optional, List, Tuple, Dict

from models import MovieData, Director, Language
from utils.logger import get_logger
from .supabase_client import supabase
from datetime import datetime

logger = get_logger(__name__)


def parse_runtime(runtime_str: Optional[str]) -> int:
    """
    Convert runtime string to minutes.
    
    Args:
        runtime_str: Runtime like '1h 56min' or None
        
    Returns:
        Runtime in minutes, 0 if invalid
    """
    if not runtime_str:
        return 0
        
    if isinstance(runtime_str, int):
        return runtime_str

    hours = minutes = 0
    
    # Extract hours
    if match := re.search(r"(\d+)h", runtime_str):
        hours = int(match.group(1))
    
    # Extract minutes
    if match := re.search(r"(\d+)min", runtime_str):
        minutes = int(match.group(1))

    return hours * 60 + minutes


def generate_movie_id(title: str, original_title: str, runtime: int = 0) -> int:
    """
    Generate stable unique ID for a movie.
    
    Args:
        title: Movie title
        original_title: Original title
        runtime: Runtime in minutes
        
    Returns:
        8-digit movie ID
    """
    # Normalize inputs
    title = title.strip().lower()
    original_title = (original_title or title).strip().lower()
    
    # Build hash string
    hash_str = f"{title}_{original_title}"
    if runtime > 0:
        hash_str += f"_{runtime}"
    
    # Generate ID from hash
    hash_bytes = hashlib.sha256(hash_str.encode()).digest()
    return int.from_bytes(hash_bytes[:4], 'big') % 100_000_000


def generate_director_id(first_name: str, last_name: str) -> int:
    """Generate stable unique ID for a director."""
    full_name = f"{first_name.strip().lower()}_{last_name.strip().lower()}"
    hash_bytes = hashlib.sha256(full_name.encode()).digest()
    return int.from_bytes(hash_bytes[:4], 'big') % 100_000_000


def parse_directors(director_str: Optional[str]) -> List[Director]:
    """Parse director string into Director objects."""
    if not director_str or director_str == "Unknown Director":
        return []
    
    directors = []
    for full_name in director_str.split("|"):
        full_name = full_name.strip()
        if not full_name or " " not in full_name:
            continue
            
        parts = full_name.split(" ", 1)
        try:
            director = Director(first_name=parts[0], last_name=parts[1])
            directors.append(director)
        except Exception as e:
            logger.warning("Invalid director name", name=full_name, error=str(e))
    
    return directors


def parse_languages(languages_data: Optional[List[dict]]) -> List[Language]:
    """Parse language data into Language objects."""
    if not languages_data:
        return []
    
    languages = []
    for lang_data in languages_data:
        try:
            if isinstance(lang_data, dict):
                lang = Language(code=lang_data.get("code", ""), 
                              label=lang_data.get("label"))
            elif isinstance(lang_data, str):
                lang = Language(code=lang_data, label=lang_data)
            else:
                continue
            
            if lang.code:  # Only add if code is not empty
                languages.append(lang)
                
        except Exception as e:
            logger.warning("Invalid language data", data=lang_data, error=str(e))
    
    return languages


def cinema_id_to_int(cinema_id: str) -> int:
    """Convert string cinema ID to integer for database."""
    if cinema_id.isdigit():
        return int(cinema_id)
    # Hash string ID to int
    return int(hashlib.sha256(cinema_id.encode()).hexdigest()[:8], 16)


def generate_circuit_id(circuit_code: str) -> int:
    """Generate stable unique ID for a circuit."""
    normalized = circuit_code.strip().lower()
    hash_bytes = hashlib.sha256(normalized.encode()).digest()
    return int.from_bytes(hash_bytes[:4], 'big') % 100_000_000


def insert_circuits() -> Dict[str, int]:
    """
    Insert all circuits into database.
    
    Returns:
        Dict mapping circuit_code to circuit_id
    """
    from allocineAPI.allocineAPI import allocineAPI
    
    api = allocineAPI()
    circuit_mapping = {}
    
    try:
        circuits_data = api.get_circuit()
        logger.info(f"Found {len(circuits_data)} circuits")
        
        circuits_to_insert = []
        for circuit in circuits_data:
            circuit_code = circuit['id']
            circuit_name = circuit['name']
            circuit_id = generate_circuit_id(circuit_code)
            
            circuits_to_insert.append({
                'id': circuit_id,
                'code': circuit_code,
                'name': circuit_name
            })
            
            circuit_mapping[circuit_code] = circuit_id
        
        # Bulk insert
        if circuits_to_insert:
            supabase.table("circuits").upsert(
                circuits_to_insert,
                on_conflict="id"
            ).execute()
            logger.info(f"Inserted/updated {len(circuits_to_insert)} circuits")
            
    except Exception as e:
        logger.error(f"Failed to insert circuits: {e}")
    
    return circuit_mapping


def get_cinema_circuit(cinema_id: str, circuit_mapping: Dict[str, int]) -> Optional[int]:
    """
    Get circuit ID for a cinema by checking all circuits.
    
    Args:
        cinema_id: Cinema ID to look for
        circuit_mapping: Dict of circuit_code -> circuit_id
        
    Returns:
        Circuit ID if found, None otherwise
    """
    from allocineAPI.allocineAPI import allocineAPI
    
    api = allocineAPI()
    
    for circuit_code, circuit_id in circuit_mapping.items():
        try:
            cinemas_in_circuit = api.get_cinema(circuit_code)
            cinema_ids = {c['id'] for c in cinemas_in_circuit}
            
            if cinema_id in cinema_ids:
                return circuit_id
                
        except Exception as e:
            logger.error(f"Error checking circuit {circuit_code}: {e}")
            continue
    
    return None


def insert_cinema(cinema_data: dict, circuit_id: Optional[int] = None) -> Optional[str]:
    """
    Insert cinema into database.
    
    Args:
        cinema_data: Cinema data from API
        circuit_id: Optional circuit ID if known
        
    Returns:
        Cinema ID if inserted, None if error
    """
    try:
        cinema_id_str = cinema_data.get("id")
        if not cinema_id_str:
            logger.error("Cinema without ID")
            return None
        
        # Convert to int for database
        cinema_id_int = cinema_id_to_int(cinema_id_str)
        
        # Check if exists
        response = supabase.table("cinemas").select("id").eq("id", cinema_id_int).execute()
        if len(response.data) > 0:
            # Update circuit_id if provided and not already set
            if circuit_id:
                supabase.table("cinemas").update({
                    "circuit_id": circuit_id
                }).eq("id", cinema_id_int).execute()
            logger.debug("Cinema already exists", cinema_id=cinema_id_str)
            return cinema_id_str
        
        # Prepare data
        data = {
            "id": cinema_id_int,
            "name": cinema_data.get("name", "Unknown"),
            "address": cinema_data.get("address"),
            "city": cinema_data.get("city", "Paris"),
            "zipcode": cinema_data.get("zipcode")
        }
        
        # Add circuit_id if provided
        if circuit_id:
            data["circuit_id"] = circuit_id
        
        # Insert
        supabase.table("cinemas").insert(data).execute()
        logger.info("Inserted cinema", 
                   name=data["name"], 
                   cinema_id=cinema_id_str,
                   circuit_id=circuit_id)
        
        return cinema_id_str
        
    except Exception as e:
        logger.error("Failed to insert cinema", 
                    cinema_id=cinema_data.get("id"),
                    error=str(e))
        return None


def insert_release(movie_id: int, release_data: dict) -> bool:
    """
    Insert movie release date avec UPSERT pour éviter les duplicates.
    """
    try:
        release_date = release_data.get("release_date", release_data.get("releaseDate"))
        if not release_date:
            return False
        
        data = {
            "movie_id": movie_id,
            "release_name": release_data.get("release_name", "Sortie française"),
            "release_date": release_date
        }
        
        # Utiliser upsert au lieu d'insert
        supabase.table("releases").upsert(
            data, 
            on_conflict="movie_id,release_date"
        ).execute()
        
        logger.debug("Upserted release", 
                    movie_id=movie_id,
                    date=release_date)
        
        return True
        
    except Exception as e:
        logger.error("Failed to upsert release", 
                    movie_id=movie_id,
                    error=str(e))
        return False


def process_cinema_screenings(cinema_id: str, date: str) -> Tuple[int, int]:
    """
    Process all screenings for a cinema on a specific date.
    OPTIMIZED VERSION with bulk operations.
    
    Args:
        cinema_id: Cinema ID (e.g., "P3757")
        date: Date in YYYY-MM-DD format
        
    Returns:
        Tuple of (movies_inserted, screenings_inserted)
    """
    from allocineAPI.allocineAPI import allocineAPI
    from .bulk_operations import bulk_insert_movies_optimized, bulk_insert_screenings
    
    logger.info("Processing cinema screenings", 
               cinema_id=cinema_id, 
               date=date)
    
    api = allocineAPI()
    
    try:
        # Get movies first
        movies_data = api.get_movies(cinema_id, date)
        
        # Bulk insert all movies at once!
        movies_inserted, movie_ids_set = bulk_insert_movies_optimized(movies_data)
        
        # Create a mapping of movie titles to IDs
        movie_title_to_id = {}
        for movie_data in movies_data:
            title = movie_data.get("title", "")
            original_title = movie_data.get("originalTitle", title)
            runtime = parse_runtime(movie_data.get("runtime", "0min"))
            movie_id = generate_movie_id(title, original_title, runtime)
            movie_title_to_id[title] = movie_id
            
            # Insert release dates if available - only the first one
            releases = movie_data.get("releases", [])
            if releases and len(releases) > 0 and releases[0].get("releaseDate"):
                insert_release(movie_id, releases[0])
        
        # Now get the actual showtimes
        showtimes_data = api.get_showtime(cinema_id, date)
        
        # Prepare all screenings for bulk insert
        all_screenings = []
        
        for showtime_entry in showtimes_data:
            movie_title = showtime_entry["title"]
            movie_id = movie_title_to_id.get(movie_title)
            
            if not movie_id:
                logger.warning("Movie ID not found for showtime", title=movie_title)
                continue
            
            # Collect all screenings
            for showtime in showtime_entry.get("showtimes", []):
                all_screenings.append({
                    "movie_id": movie_id,
                    "date": date,
                    "time": showtime.get("startsAt"),
                    "version": showtime.get("diffusionVersion")
                })
        
        # Bulk insert all screenings at once!
        screenings_inserted = bulk_insert_screenings(all_screenings, cinema_id)
        
        logger.info("Cinema processing complete",
                   cinema_id=cinema_id,
                   movies_inserted=movies_inserted,
                   screenings_inserted=screenings_inserted)
        
        return movies_inserted, screenings_inserted
        
    except Exception as e:
        logger.error("Failed to process cinema screenings",
                    cinema_id=cinema_id,
                    error=str(e))
        return 0, 0


def insert_cinema_from_location(location_id: str) -> List[str]:
    """
    Get and insert cinemas from a location (city, department, circuit).
    
    Args:
        location_id: Location ID (e.g., "ville-75056" for Paris)
        
    Returns:
        List of cinema IDs inserted
    """
    from allocineAPI.allocineAPI import allocineAPI
    
    api = allocineAPI()
    cinema_ids = []
    
    try:
        cinemas_data = api.get_cinema(location_id)
        
        for cinema_data in cinemas_data:
            cinema_id = insert_cinema({
                "id": cinema_data["id"],
                "name": cinema_data["name"],
                "address": cinema_data["address"],
                "city": "Paris",  # You might want to extract this from location_id
                "zipcode": None   # Not provided by API
            })
            
            if cinema_id:
                cinema_ids.append(cinema_id)
        
        logger.info("Inserted cinemas from location",
                   location_id=location_id,
                   count=len(cinema_ids))
        
        return cinema_ids
        
    except Exception as e:
        logger.error("Failed to get cinemas from location",
                    location_id=location_id,
                    error=str(e))
        return []


# Exposer seulement les fonctions publiques nécessaires
__all__ = [
    'parse_runtime',
    'generate_movie_id', 
    'generate_director_id',
    'parse_directors',
    'parse_languages',
    'cinema_id_to_int',
    'insert_cinema',
    'insert_release',
    'process_cinema_screenings',
    'insert_cinema_from_location'
]