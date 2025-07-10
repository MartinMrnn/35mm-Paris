"""
Opérations bulk optimisées pour Supabase.
Remplace les insertions une par une par des batch inserts.
"""
from typing import List, Dict, Set, Tuple
from datetime import datetime

from utils.logger import get_logger
from .supabase_client import supabase
from .insert_logic import generate_movie_id, parse_runtime, generate_director_id

logger = get_logger(__name__)

# Taille des batches pour les insertions
BATCH_SIZE = 100


def bulk_insert_movies_optimized(movies_data: List[dict]) -> Tuple[int, Set[int]]:
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
                runtime
            )
            movie_ids.add(movie_id)
            
            # Préparer les données du film
            movies_to_insert.append({
                "id": movie_id,
                "title": movie_data["title"],
                "original_title": movie_data.get("originalTitle", movie_data["title"]),
                "synopsis": movie_data.get("synopsisFull", movie_data.get("synopsis")),
                "poster_url": movie_data.get("urlPoster", movie_data.get("poster_url")),
                "runtime": runtime,
                "has_dvd_release": movie_data.get("hasDvdRelease", False),
                "is_premiere": movie_data.get("isPremiere", False),
                "weekly_outing": movie_data.get("weeklyOuting", False)
            })
            
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
                            "last_name": parts[1]
                        }
                        movie_directors_links.append({
                            "movie_id": movie_id,
                            "director_id": director_id
                        })
            
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
                    languages_to_insert[code] = {
                        "code": code,
                        "label": label
                    }
                    movie_languages_links.append({
                        "movie_id": movie_id,
                        "code": code
                    })
                    
        except Exception as e:
            logger.error(f"Erreur préparation film {movie_data.get('title')}: {e}")
    
    # Maintenant, faire les insertions par batch
    inserted_count = 0
    
    try:
        # 1. Insérer les films (ignorer les conflits)
        for i in range(0, len(movies_to_insert), BATCH_SIZE):
            batch = movies_to_insert[i:i + BATCH_SIZE]
            result = supabase.table("movies").upsert(batch, on_conflict="id").execute()
            inserted_count += len([m for m in result.data if m])
            logger.info(f"Batch films {i//BATCH_SIZE + 1}: {len(batch)} films")
        
        # 2. Insérer les réalisateurs
        if directors_to_insert:
            directors_list = list(directors_to_insert.values())
            for i in range(0, len(directors_list), BATCH_SIZE):
                batch = directors_list[i:i + BATCH_SIZE]
                supabase.table("directors").upsert(batch, on_conflict="id").execute()
        
        # 3. Insérer les langues
        if languages_to_insert:
            languages_list = list(languages_to_insert.values())
            supabase.table("languages").upsert(languages_list, on_conflict="code").execute()
        
        # 4. Créer les liens
        if movie_directors_links:
            for i in range(0, len(movie_directors_links), BATCH_SIZE):
                batch = movie_directors_links[i:i + BATCH_SIZE]
                supabase.table("movie_directors").upsert(
                    batch, 
                    on_conflict="movie_id,director_id"
                ).execute()
        
        if movie_languages_links:
            for i in range(0, len(movie_languages_links), BATCH_SIZE):
                batch = movie_languages_links[i:i + BATCH_SIZE]
                supabase.table("movie_languages").upsert(
                    batch,
                    on_conflict="movie_id,code"
                ).execute()
        
        logger.info(f"Bulk insert terminé: {inserted_count} films insérés")
        
    except Exception as e:
        logger.error(f"Erreur lors du bulk insert: {e}")
    
    return inserted_count, movie_ids


def bulk_insert_screenings(screenings_data: List[Dict], cinema_id: str) -> int:
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
    
    from .insert_logic import cinema_id_to_int
    cinema_id_int = cinema_id_to_int(cinema_id)
    
    # Préparer les données
    screenings_to_insert = []
    
    for screening in screenings_data:
        try:
            time_str = screening.get("time", screening.get("starts_at"))
            
            # Extraire l'heure si c'est un datetime
            if time_str and "T" in time_str:
                time_str = time_str.split("T")[1].split("+")[0].split("Z")[0]
            
            screenings_to_insert.append({
                "movie_id": screening["movie_id"],
                "cinema_id": cinema_id_int,
                "date": screening["date"],
                "starts_at": time_str,
                "diffusion_version": screening.get("version", screening.get("diffusion_version"))
            })
            
        except Exception as e:
            logger.error(f"Erreur préparation séance: {e}")
    
    # Insérer par batch
    inserted_count = 0
    
    try:
        for i in range(0, len(screenings_to_insert), BATCH_SIZE):
            batch = screenings_to_insert[i:i + BATCH_SIZE]
            
            # Utiliser upsert pour éviter les erreurs de duplication
            result = supabase.table("screenings").upsert(
                batch,
                on_conflict="movie_id,cinema_id,date,starts_at"
            ).execute()
            
            inserted_count += len(result.data)
            
    except Exception as e:
        logger.error(f"Erreur bulk insert screenings: {e}")
    
    return inserted_count