"""
Database insertion logic for 35mm Paris.
Modern, type-safe, with proper error handling and bulk operations.
"""

import hashlib
import re

from models import Director, Language
from utils.logger import get_logger

from .supabase_client import supabase

logger = get_logger(__name__)

# Batch size for bulk operations
BATCH_SIZE = 100


def parse_runtime(runtime_str: str | None) -> int:
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
    return int.from_bytes(hash_bytes[:4], "big") % 100_000_000


def generate_director_id(first_name: str, last_name: str) -> int:
    """Generate stable unique ID for a director."""
    full_name = f"{first_name.strip().lower()}_{last_name.strip().lower()}"
    hash_bytes = hashlib.sha256(full_name.encode()).digest()
    return int.from_bytes(hash_bytes[:4], "big") % 100_000_000


def generate_circuit_id(circuit_code: str) -> int:
    """Generate stable unique ID for a circuit."""
    normalized = circuit_code.strip().lower()
    hash_bytes = hashlib.sha256(normalized.encode()).digest()
    return int.from_bytes(hash_bytes[:4], "big") % 100_000_000


def cinema_id_to_int(cinema_id: str) -> int:
    """Convert string cinema ID to integer for database."""
    if cinema_id.isdigit():
        return int(cinema_id)
    # Hash string ID to int
    return int(hashlib.sha256(cinema_id.encode()).hexdigest()[:8], 16)


def parse_directors(director_str: str | None) -> list[Director]:
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


def parse_languages(languages_data: list[dict] | None) -> list[Language]:
    """Parse language data into Language objects."""
    if not languages_data:
        return []

    languages = []
    for lang_data in languages_data:
        try:
            if isinstance(lang_data, dict):
                lang = Language(
                    code=lang_data.get("code", ""), label=lang_data.get("label")
                )
            elif isinstance(lang_data, str):
                lang = Language(code=lang_data, label=lang_data)
            else:
                continue

            if lang.code:  # Only add if code is not empty
                languages.append(lang)

        except Exception as e:
            logger.warning("Invalid language data", data=lang_data, error=str(e))

    return languages


def insert_cinema(cinema_data: dict, circuit_id: int | None = None) -> str | None:
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
        response = (
            supabase.table("cinemas").select("id").eq("id", cinema_id_int).execute()
        )
        if len(response.data) > 0:
            # Update circuit_id if provided and not already set
            if circuit_id:
                supabase.table("cinemas").update({"circuit_id": circuit_id}).eq(
                    "id", cinema_id_int
                ).execute()
            logger.debug("Cinema already exists", cinema_id=cinema_id_str)
            return cinema_id_str

        # Prepare data
        data = {
            "id": cinema_id_int,
            "name": cinema_data.get("name", "Unknown"),
            "address": cinema_data.get("address"),
            "city": cinema_data.get("city", "Paris"),
            "zipcode": cinema_data.get("zipcode"),
        }

        # Add circuit_id if provided
        if circuit_id:
            data["circuit_id"] = circuit_id

        # Insert
        supabase.table("cinemas").insert(data).execute()
        logger.info(
            "Inserted cinema",
            name=data["name"],
            cinema_id=cinema_id_str,
            circuit_id=circuit_id,
        )

        return cinema_id_str

    except Exception as e:
        logger.error(
            "Failed to insert cinema", cinema_id=cinema_data.get("id"), error=str(e)
        )
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
            "release_date": release_date,
        }

        # Utiliser upsert au lieu d'insert
        supabase.table("releases").upsert(
            data, on_conflict="movie_id,release_date"
        ).execute()

        logger.debug("Upserted release", movie_id=movie_id, date=release_date)

        return True

    except Exception as e:
        logger.error("Failed to upsert release", movie_id=movie_id, error=str(e))
        return False


def bulk_insert_movies(movies_data: list[dict]) -> tuple[int, set[int]]:
    """
    Insère plusieurs films en une seule opération.

    Returns:
        Tuple (nombre de films insérés, set des movie_ids)
    """
    if not movies_data:
        return 0, set()

    # Préparer tous les films
    movies_to_insert = []
    movie_ids = set()
    directors_to_insert = {}  # key: director_id, value: director_data
    languages_to_insert = {}  # key: code, value: language_data
    movie_directors_links = []
    movie_languages_links = []

    for movie_data in movies_data:
        try:
            # Générer l'ID du film
            runtime = parse_runtime(movie_data.get("runtime", "0"))
            movie_id = generate_movie_id(
                movie_data["title"],
                movie_data.get("originalTitle", movie_data["title"]),
                runtime,
            )
            movie_ids.add(movie_id)

            # Préparer les données du film
            movies_to_insert.append(
                {
                    "id": movie_id,
                    "title": movie_data["title"],
                    "original_title": movie_data.get(
                        "originalTitle", movie_data["title"]
                    ),
                    "synopsis": movie_data.get(
                        "synopsisFull", movie_data.get("synopsis")
                    ),
                    "poster_url": movie_data.get(
                        "urlPoster", movie_data.get("poster_url")
                    ),
                    "runtime": runtime,
                    "has_dvd_release": movie_data.get("hasDvdRelease", False),
                    "is_premiere": movie_data.get("isPremiere", False),
                    "weekly_outing": movie_data.get("weeklyOuting", False),
                }
            )

            # Traiter les réalisateurs
            director_str = movie_data.get("director", "")
            if director_str and director_str != "Unknown Director":
                for full_name in director_str.split("|"):
                    full_name = full_name.strip()
                    if " " in full_name:
                        parts = full_name.split(" ", 1)
                        director_id = generate_director_id(parts[0], parts[1])
                        directors_to_insert[director_id] = {
                            "id": director_id,
                            "first_name": parts[0],
                            "last_name": parts[1],
                        }
                        movie_directors_links.append(
                            {"movie_id": movie_id, "director_id": director_id}
                        )

            # Traiter les langues
            languages = movie_data.get("languages", [])
            for lang in languages:
                if isinstance(lang, dict):
                    code = lang.get("code", "")
                    label = lang.get("label", code)
                elif isinstance(lang, str):
                    code = lang
                    label = lang
                else:
                    continue

                if code:
                    languages_to_insert[code] = {"code": code, "label": label}
                    movie_languages_links.append({"movie_id": movie_id, "code": code})

        except Exception as e:
            logger.error(f"Erreur préparation film {movie_data.get('title')}: {e}")

    # Maintenant, faire les insertions par batch
    inserted_count = 0

    try:
        # 1. Insérer les films (ignorer les conflits)
        for i in range(0, len(movies_to_insert), BATCH_SIZE):
            batch = movies_to_insert[i : i + BATCH_SIZE]
            result = supabase.table("movies").upsert(batch, on_conflict="id").execute()
            inserted_count += len([m for m in result.data if m])
            logger.info(f"Batch films {i//BATCH_SIZE + 1}: {len(batch)} films")

        # 2. Insérer les réalisateurs
        if directors_to_insert:
            directors_list = list(directors_to_insert.values())
            for i in range(0, len(directors_list), BATCH_SIZE):
                batch = directors_list[i : i + BATCH_SIZE]
                supabase.table("directors").upsert(batch, on_conflict="id").execute()

        # 3. Insérer les langues
        if languages_to_insert:
            languages_list = list(languages_to_insert.values())
            supabase.table("languages").upsert(
                languages_list, on_conflict="code"
            ).execute()

        # 4. Créer les liens
        if movie_directors_links:
            for i in range(0, len(movie_directors_links), BATCH_SIZE):
                batch = movie_directors_links[i : i + BATCH_SIZE]
                supabase.table("movie_directors").upsert(
                    batch, on_conflict="movie_id,director_id"
                ).execute()

        if movie_languages_links:
            for i in range(0, len(movie_languages_links), BATCH_SIZE):
                batch = movie_languages_links[i : i + BATCH_SIZE]
                supabase.table("movie_languages").upsert(
                    batch, on_conflict="movie_id,code"
                ).execute()

        logger.info(f"Bulk insert terminé: {inserted_count} films insérés")

    except Exception as e:
        logger.error(f"Erreur lors du bulk insert: {e}")

    return inserted_count, movie_ids


def bulk_insert_screenings(screenings_data: list[dict], cinema_id: str) -> int:
    """
    Insère plusieurs séances en une seule opération.

    Args:
        screenings_data: Liste des séances avec movie_id, date, time, version
        cinema_id: ID du cinéma

    Returns:
        Nombre de séances insérées
    """
    if not screenings_data:
        return 0

    cinema_id_int = cinema_id_to_int(cinema_id)

    # Préparer les données
    screenings_to_insert = []

    for screening in screenings_data:
        try:
            time_str = screening.get("time", screening.get("starts_at"))

            # Extraire l'heure si c'est un datetime
            if time_str and "T" in time_str:
                time_str = time_str.split("T")[1].split("+")[0].split("Z")[0]

            screenings_to_insert.append(
                {
                    "movie_id": screening["movie_id"],
                    "cinema_id": cinema_id_int,
                    "date": screening["date"],
                    "starts_at": time_str,
                    "diffusion_version": screening.get(
                        "version", screening.get("diffusion_version")
                    ),
                }
            )

        except Exception as e:
            logger.error(f"Erreur préparation séance: {e}")

    # Insérer par batch
    inserted_count = 0

    try:
        for i in range(0, len(screenings_to_insert), BATCH_SIZE):
            batch = screenings_to_insert[i : i + BATCH_SIZE]

            # Utiliser upsert pour éviter les erreurs de duplication
            result = (
                supabase.table("screenings")
                .upsert(batch, on_conflict="movie_id,cinema_id,date,starts_at")
                .execute()
            )

            inserted_count += len(result.data)

    except Exception as e:
        logger.error(f"Erreur bulk insert screenings: {e}")

    return inserted_count


def process_cinema_screenings(cinema_id: str, date: str) -> tuple[int, int]:
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

    logger.info("Processing cinema screenings", cinema_id=cinema_id, date=date)

    api = allocineAPI()

    try:
        # Get movies first
        movies_data = api.get_movies(cinema_id, date)

        # Bulk insert all movies at once!
        movies_inserted, movie_ids_set = bulk_insert_movies(movies_data)

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
                all_screenings.append(
                    {
                        "movie_id": movie_id,
                        "date": date,
                        "time": showtime.get("startsAt"),
                        "version": showtime.get("diffusionVersion"),
                    }
                )

        # Bulk insert all screenings at once!
        screenings_inserted = bulk_insert_screenings(all_screenings, cinema_id)

        logger.info(
            "Cinema processing complete",
            cinema_id=cinema_id,
            movies_inserted=movies_inserted,
            screenings_inserted=screenings_inserted,
        )

        return movies_inserted, screenings_inserted

    except Exception as e:
        logger.error(
            "Failed to process cinema screenings", cinema_id=cinema_id, error=str(e)
        )
        return 0, 0


# Exposer seulement les fonctions publiques nécessaires
__all__ = [
    "parse_runtime",
    "generate_movie_id",
    "generate_director_id",
    "generate_circuit_id",
    "parse_directors",
    "parse_languages",
    "cinema_id_to_int",
    "insert_cinema",
    "insert_release",
    "process_cinema_screenings",
    "bulk_insert_movies",
    "bulk_insert_screenings",
]
