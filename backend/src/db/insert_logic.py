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
    Generates a stable unique numeric ID for a movie,
    based on its title, original title, and optionally its runtime.
    """

    # Clean and normalize input strings
    title = (title or "").strip().lower()
    original_title = (original_title or "").strip().lower()

    # Build the base string to hash
    base_str = f"{title}_{original_title}"

    # Optionally add runtime if available and valid
    if runtime and isinstance(runtime, int) and runtime > 0:
        base_str += f"_{runtime}"

    # Generate SHA256 hash, convert to int, reduce to 8 digits
    return int(hashlib.sha256(base_str.encode()).hexdigest(), 16) % (10 ** 8)

def generate_director_id(first_name: str, last_name: str):
    """
    Generates a stable unique numeric ID for a director based on their full name.
    """
    full_name = f"{(first_name or '').strip().lower()}_{(last_name or '').strip().lower()}"
    return int(hashlib.sha256(full_name.encode()).hexdigest(), 16) % (10 ** 8)

def insert_movie(movie: dict) -> bool:
    """
    Inserts a movie into Supabase using a generated unique ID,
    only if it is not already present. Returns True if inserted, False otherwise.
    """
    title = movie.get("title", "Unknown Title")
    original_title = movie.get("originalTitle", "Unknown Original Title")
    runtime = parse_runtime(movie.get("runtime", "0min"))
    movie_id = generate_movie_id(title, original_title, runtime)

    # Check for duplicates via count
    response = supabase.table("movies").select("id", count="exact").eq("id", movie_id).execute()
    if response.count and response.count > 0:
        print(f"⏭️ Movie already exists: {title} (ID: {movie_id})")
        return False

    # Prepare data for insertion
    data = {
        "id": movie_id,
        "title": title,
        "original_title": original_title,
        "synopsis": movie.get("synopsisFull"),
        "poster_url": movie.get("urlPoster"),
        "runtime": runtime,
        "has_dvd_release": movie.get("hasDvdRelease", False),
        "is_premiere": movie.get("isPremiere", False),
        "weekly_outing": movie.get("weeklyOuting", False)
    }

    # Insert into Supabase
    supabase.table("movies").insert(data).execute()
    print(f"✅ Inserted movie: {title} (ID: {movie_id})")
    return True

def insert_directors(movie: dict, movie_id: int):
    """
    Inserts all directors of a movie into the 'directors' table and links them via 'movie_directors'.
    """
    director_str = movie.get("director", "")
    if not director_str or director_str == "Unknown Director":
        return

    director_names = [name.strip() for name in director_str.split("|")]

    for full_name in director_names:
        if not full_name or " " not in full_name:
            continue

        first_name, last_name = full_name.split(" ", 1)
        director_id = generate_director_id(first_name, last_name)

        # Check and insert into 'directors'
        exists = supabase.table("directors").select("id", count="exact") \
            .eq("first_name", first_name).eq("last_name", last_name).execute()
        if not exists.count or exists.count == 0:
            supabase.table("directors").insert({
                "id": director_id,
                "first_name": first_name,
                "last_name": last_name
            }).execute()
            print(f"🎬 Inserted director: {first_name} {last_name} (ID: {director_id})")

        # Check and insert into 'movie_directors'
        link_exists = supabase.table("movie_directors").select("movie_id", count="exact") \
            .eq("movie_id", movie_id).eq("director_id", director_id).execute()
        if not link_exists.count or link_exists.count == 0:
            supabase.table("movie_directors").insert({
                "movie_id": movie_id,
                "director_id": director_id
            }).execute()
            print(f"🔗 Linked movie {movie_id} to director {director_id}")
