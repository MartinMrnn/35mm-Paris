import sys
from pathlib import Path
from datetime import datetime

# 🔧 Ajouter backend/src au path d'import
sys.path.append(str(Path(__file__).resolve().parent / "backend" / "src"))

from db.insert_logic import insert_movie
from allocineAPI.allocineAPI import allocineAPI

def main():
    print("🎬 Lancement de l'import des films depuis l'API Allociné")

    # Initialisation de l'API
    api = allocineAPI()

    # ID d'un cinéma parisien connu (ex : UGC Gobelins)
    cinema_id = "P3757"
    date_str = datetime.today().strftime("%Y-%m-%d")

    try:
        movies = api.get_movies(cinema_id, date_str)
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des films : {e}")
        return

    print(f"📅 Films du {date_str} pour le cinéma {cinema_id} : {len(movies)} films récupérés")

    inserted, skipped = 0, 0
    for movie in movies:
        if not movie.get("title"):
            print("⚠️ Film sans titre — ignoré.")
            skipped += 1
            continue
        try:
            if insert_movie(movie):
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"❌ Erreur à l'insertion du film {movie.get('title')}: {e}")
            skipped += 1

    print(f"\n✅ Fin de l'import : {inserted} films insérés, {skipped} ignorés ou déjà présents.")

if __name__ == "__main__":
    main()
