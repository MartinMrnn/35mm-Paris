"""
Database insertion logic for 35mm Paris.
Modern, type-safe, with proper error handling.
"""
import re
import hashlib
from typing import Optional, List, Tuple

from models import MovieData, Director, Language, Cinema, Screening
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


def movie_exists(movie_id: int) -> bool:
    """Check if movie already exists in database."""
    try:
        response = supabase.table("movies").select("id").eq("id", movie_id).execute()
        return len(response.data) > 0
    except Exception as e:
        logger.error("Error checking movie existence", movie_id=movie_id, error=str(e))
        return False


def insert_movie(movie_data: dict) -> Optional[int]:
    """
    Insert movie into database.
    
    Args:
        movie_data: Raw movie data from API
        
    Returns:
        Movie ID if inserted, None if already exists or error
    """
    try:
        # Validate data
        movie = MovieData(**movie_data)
        runtime = parse_runtime(movie.runtime)
        movie_id = generate_movie_id(movie.title, movie.originalTitle or movie.title, runtime)
        
        # Check if exists
        if movie_exists(movie_id):
            logger.info("Movie already exists", title=movie.title, movie_id=movie_id)
            return None
        
        # Prepare data for insertion
        data = {
            "id": movie_id,
            "title": movie.title,
            "original_title": movie.originalTitle or movie.title,
            "synopsis": movie.synopsis,
            "poster_url": movie.poster_url,
            "runtime": runtime,
            "has_dvd_release": movie.has_dvd_release,
            "is_premiere": movie.is_premiere,
            "weekly_outing": movie.weekly_outing
        }
        
        # Insert
        supabase.table("movies").insert(data).execute()
        logger.info("Inserted movie", title=movie.title, movie_id=movie_id)
        
        # Insert related data
        _insert_directors(movie, movie_id)
        _insert_languages(movie, movie_id)
        
        return movie_id
        
    except Exception as e:
        logger.error("Failed to insert movie", 
                    title=movie_data.get('title', 'Unknown'),
                    error=str(e))
        return None


def _parse_directors(director_str: Optional[str]) -> List[Director]:
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


def _insert_directors(movie: MovieData, movie_id: int) -> None:
    """Insert directors and link to movie."""
    directors = _parse_directors(movie.director)
    
    for director in directors:
        try:
            director_id = generate_director_id(director.first_name, director.last_name)
            
            # Insert director if not exists (upsert)
            supabase.table("directors").upsert({
                "id": director_id,
                "first_name": director.first_name,
                "last_name": director.last_name
            }).execute()
            
            # Link to movie (ignore conflicts)
            supabase.table("movie_directors").upsert({
                "movie_id": movie_id,
                "director_id": director_id
            }).execute()
            
            logger.debug("Linked director to movie", 
                        director=f"{director.first_name} {director.last_name}",
                        movie_id=movie_id)
                        
        except Exception as e:
            logger.error("Failed to insert director", 
                        director=f"{director.first_name} {director.last_name}",
                        error=str(e))


def _parse_languages(languages_data: Optional[List[dict]]) -> List[Language]:
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


def _insert_languages(movie: MovieData, movie_id: int) -> None:
    """Insert languages and link to movie."""
    languages = _parse_languages(movie.languages)
    
    for language in languages:
        try:
            # Insert language if not exists
            supabase.table("languages").upsert({
                "code": language.code,
                "label": language.label
            }).execute()
            
            # Link to movie
            supabase.table("movie_languages").upsert({
                "movie_id": movie_id,
                "code": language.code
            }).execute()
            
            logger.debug("Linked language to movie", 
                        language=language.code,
                        movie_id=movie_id)
                        
        except Exception as e:
            logger.error("Failed to insert language", 
                        language=language.code,
                        error=str(e))


def bulk_insert_movies(movies_data: List[dict]) -> Tuple[int, int]:
    """
    Insert multiple movies.
    
    Args:
        movies_data: List of movie data from API
        
    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    inserted = 0
    skipped = 0
    
    logger.info("Starting bulk insert", total_movies=len(movies_data))
    
    for movie_data in movies_data:
        if not movie_data.get("title"):
            logger.warning("Skipping movie without title")
            skipped += 1
            continue
        
        movie_id = insert_movie(movie_data)
        if movie_id:
            inserted += 1
        else:
            skipped += 1
    
    logger.info("Bulk insert complete", 
                inserted=inserted, 
                skipped=skipped)
    
    return inserted, skipped


def cinema_id_to_int(cinema_id: str) -> int:
    """Convert string cinema ID to integer for database."""
    if cinema_id.isdigit():
        return int(cinema_id)
    # Hash string ID to int
    return int(hashlib.sha256(cinema_id.encode()).hexdigest()[:8], 16)


def insert_cinema(cinema_data: dict) -> Optional[str]:
    """
    Insert cinema into database.
    
    Args:
        cinema_data: Cinema data from API
        
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
        
        # Insert
        supabase.table("cinemas").insert(data).execute()
        logger.info("Inserted cinema", 
                   name=data["name"], 
                   cinema_id=cinema_id_str)
        
        return cinema_id_str
        
    except Exception as e:
        logger.error("Failed to insert cinema", 
                    cinema_id=cinema_data.get("id"),
                    error=str(e))
        return None


def insert_screening(screening_data: dict, movie_id: int, cinema_id: str) -> bool:
    """
    Insert screening into database.
    
    Args:
        screening_data: Screening data with date and time
        movie_id: Movie ID
        cinema_id: Cinema ID (string from API)
        
    Returns:
        True if inserted, False otherwise
    """
    try:
        # Convert cinema ID to int
        cinema_id_int = cinema_id_to_int(cinema_id)
        
        # Extract date and time
        date_str = screening_data.get("date")
        time_str = screening_data.get("time", screening_data.get("starts_at"))
        version = screening_data.get("version", screening_data.get("diffusion_version"))
        
        if not date_str:
            logger.error("Screening without date")
            return False
        
        # Extract just the time part if it's a datetime string
        if time_str and "T" in time_str:
            # Format: "2025-07-04T13:15:00" -> "13:15:00"
            time_str = time_str.split("T")[1]
            # Remove timezone if present
            if "+" in time_str:
                time_str = time_str.split("+")[0]
            if "Z" in time_str:
                time_str = time_str.replace("Z", "")
        
        # Prepare data
        data = {
            "movie_id": movie_id,
            "cinema_id": cinema_id_int,
            "date": date_str,
            "starts_at": time_str,
            "diffusion_version": version
        }
        
        # Upsert (to avoid duplicate constraint violations)
        # First check if exists
        existing = supabase.table("screenings").select("id").eq("movie_id", movie_id).eq("cinema_id", cinema_id_int).eq("date", date_str).eq("starts_at", time_str).execute()
        
        if len(existing.data) == 0:
            supabase.table("screenings").insert(data).execute()
            logger.debug("Inserted screening", 
                        movie_id=movie_id,
                        cinema_id=cinema_id,
                        date=date_str,
                        time=time_str)
        else:
            logger.debug("Screening already exists", 
                        movie_id=movie_id,
                        cinema_id=cinema_id,
                        date=date_str,
                        time=time_str)
        
        return True
        
    except Exception as e:
        logger.error("Failed to insert screening", 
                    movie_id=movie_id,
                    cinema_id=cinema_id,
                    error=str(e))
        return False


def insert_release(movie_id: int, release_data: dict) -> bool:
    """
    Insert movie release date.
    
    Args:
        movie_id: Movie ID
        release_data: Release information
        
    Returns:
        True if inserted, False otherwise
    """
    try:
        release_date = release_data.get("release_date", release_data.get("releaseDate"))
        if not release_date:
            return False
        
        # Prepare data
        data = {
            "movie_id": movie_id,
            "release_name": release_data.get("release_name", "Sortie française"),
            "release_date": release_date
        }
        
        # Insert
        supabase.table("releases").insert(data).execute()
        logger.debug("Inserted release", 
                    movie_id=movie_id,
                    date=release_date)
        
        return True
        
    except Exception as e:
        logger.error("Failed to insert release", 
                    movie_id=movie_id,
                    error=str(e))
        return False


def process_cinema_screenings(cinema_id: str, date: str) -> Tuple[int, int]:
    """
    Process all screenings for a cinema on a specific date.
    
    Args:
        cinema_id: Cinema ID (e.g., "P3757")
        date: Date in YYYY-MM-DD format
        
    Returns:
        Tuple of (movies_inserted, screenings_inserted)
    """
    from allocineAPI.allocineAPI import allocineAPI
    
    logger.info("Processing cinema screenings", 
               cinema_id=cinema_id, 
               date=date)
    
    api = allocineAPI()
    
    try:
        movies_inserted = 0
        screenings_inserted = 0
        
        # Get movies first
        movies_data = api.get_movies(cinema_id, date)
        
        # Create a mapping of movie titles to IDs for later
        movie_title_to_id = {}
        
        for movie_data in movies_data:
            # Insert movie OR get existing ID
            movie_id = insert_movie(movie_data)
            if not movie_id:
                # Movie already exists, need to get its ID
                title = movie_data.get("title", "")
                original_title = movie_data.get("originalTitle", title)
                runtime = parse_runtime(movie_data.get("runtime", "0min"))
                movie_id = generate_movie_id(title, original_title, runtime)
            else:
                movies_inserted += 1
            
            # Always add to mapping
            movie_title_to_id[movie_data["title"]] = movie_id
            
            # Insert release dates if available
            releases = movie_data.get("releases", [])
            for release in releases:
                if release.get("releaseDate"):
                    insert_release(movie_id, release)
        
        # Now get the actual showtimes
        showtimes_data = api.get_showtime(cinema_id, date)
        
        for showtime_entry in showtimes_data:
            movie_title = showtime_entry["title"]
            movie_id = movie_title_to_id.get(movie_title)
            
            if not movie_id:
                logger.warning("Movie ID not found for showtime", title=movie_title)
                continue
            
            # Insert each showtime
            for showtime in showtime_entry.get("showtimes", []):
                screening_data = {
                    "date": date,
                    "time": showtime.get("startsAt"),
                    "version": showtime.get("diffusionVersion")
                }
                
                if insert_screening(screening_data, movie_id, cinema_id):
                    screenings_inserted += 1
        
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