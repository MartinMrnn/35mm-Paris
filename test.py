import sys
from pathlib import Path
from datetime import datetime

# Add backend/src to import path
sys.path.append(str(Path(__file__).resolve().parent / "backend" / "src"))

from db.insert_logic import (
    insert_movie,
    insert_directors,
    insert_languages,
    generate_movie_id,
    parse_runtime,
)
from allocineAPI.allocineAPI import allocineAPI

def main():
    print("🎬 Launching movie import from Allociné API")

    # Init Allociné API
    api = allocineAPI()

    # Cinema ID (example: UGC Gobelins, Paris)
    cinema_id = "P3757"
    date_str = datetime.today().strftime("%Y-%m-%d")

    try:
        movies = api.get_movies(cinema_id, date_str)
    except Exception as e:
        print(f"❌ Error fetching movies: {e}")
        return

    print(f"📅 Movies for {date_str} at cinema {cinema_id} — {len(movies)} found")

    inserted, skipped = 0, 0
    for movie in movies:
        if not movie.get("title"):
            print("⚠️ Skipping movie with no title")
            skipped += 1
            continue

        try:
            # Insert movie or skip if already exists
            movie_inserted = insert_movie(movie)
            if movie_inserted:
                inserted += 1
            else:
                skipped += 1

            # Compute movie_id for relations
            title = movie.get("title", "")
            original_title = movie.get("originalTitle", "")
            runtime = parse_runtime(movie.get("runtime", "0min"))
            movie_id = generate_movie_id(title, original_title, runtime)

            # Insert related data
            insert_directors(movie, movie_id)
            insert_languages(movie, movie_id)

        except Exception as e:
            print(f"❌ Error processing movie {movie.get('title')}: {e}")
            skipped += 1

    print(f"\n✅ Import complete: {inserted} movies inserted, {skipped} skipped or already present.")

if __name__ == "__main__":
    main()
