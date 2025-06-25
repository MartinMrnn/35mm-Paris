from .supabase_client import supabase
import re
import hashlib

def parse_runtime(runtime_str):
    """
    Converts a string like '1h 56min' to integer minutes.
    """
    if isinstance(runtime_str, int):
        return runtime_str

    hours = minutes = 0
    match = re.search(r"(\d+)h", runtime_str)
    if match:
        hours = int(match.group(1))

    match = re.search(r"(\d+)min", runtime_str)
    if match:
        minutes = int(match.group(1))

    return hours * 60 + minutes

def generate_movie_id(title: str, original_title: str, runtime=None):
    """
    Génère un identifiant numérique unique et stable pour un film,
    basé sur le titre, le titre original, et éventuellement la durée (runtime).
    """

    # Nettoyage et normalisation des chaînes
    title = (title or "").strip().lower()
    original_title = (original_title or "").strip().lower()

    # Construction de la base de l'empreinte
    base_str = f"{title}_{original_title}"

    # Ajout du runtime si disponible et significatif
    if runtime and isinstance(runtime, int) and runtime > 0:
        base_str += f"_{runtime}"

    # Hash SHA256, converti en int, puis réduit à 8 chiffres
    return int(hashlib.sha256(base_str.encode()).hexdigest(), 16) % (10 ** 8)

def insert_movie(movie: dict) -> bool:
    """
    Insert a movie into Supabase using a generated unique ID, if not already present.
    Retourne True si le film est inséré, False s'il est ignoré (doublon ou invalide).
    """
    title = movie.get("title", "Unknown Title")
    original_title = movie.get("originalTitle", "Unknown Original Title")
    runtime = parse_runtime(movie.get("runtime", "0min"))
    movie_id = generate_movie_id(title, original_title, runtime)

    # Vérification des doublons via count
    response = supabase.table("movies").select("id", count="exact").eq("id", movie_id).execute()
    if response.count and response.count > 0:
        print(f"⏭️ Film déjà présent : {title} (ID: {movie_id})")
        return False

    # Préparation des données
    data = {
        "id": movie_id,
        "title": title,
        "original_title": original_title,
        "synopsis": movie.get("synopsisFull"),
        "poster_url": movie.get("urlPoster"),
        "runtime": parse_runtime(movie.get("runtime", "0min")),
        "has_dvd_release": movie.get("hasDvdRelease", False),
        "is_premiere": movie.get("isPremiere", False),
        "weekly_outing": movie.get("weeklyOuting", False)
    }

    # Insertion
    supabase.table("movies").insert(data).execute()
    print(f"✅ Film inséré : {title} (ID: {movie_id})")
    return True
