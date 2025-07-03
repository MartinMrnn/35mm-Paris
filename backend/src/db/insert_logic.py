"""
Database insertion logic for 35mm Paris.
Modern, type-safe, with proper error handling.
"""
import re
import hashlib
from typing import Optional, List, Tuple, Union

from models import MovieData, Director, Language
from utils.logger import get_logger
from .supabase_client import supabase

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


def _parse_languages(languages_data: Optional[List[Union[dict, str]]]) -> List[Language]:
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