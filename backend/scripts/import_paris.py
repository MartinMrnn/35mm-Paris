#!/usr/bin/env python3
"""
Script simple et robuste pour importer les données des cinémas parisiens.
Usage: python import_paris.py [--days 7] [--test]
"""
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Set

# Add src to path
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.insert_logic import (
    process_cinema_screenings, 
    insert_cinema,
    cinema_id_to_int
)
from db.supabase_client import supabase
from utils.logger import get_logger
from allocineAPI.allocineAPI import allocineAPI

logger = get_logger(__name__)

# Configuration
PARIS_LOCATION_ID = "ville-115755"  # ID officiel d'Allocine pour Paris
DELAY_BETWEEN_REQUESTS = 0.5  # secondes entre chaque requête
MAX_RETRIES = 3


def get_paris_cinemas() -> List[dict]:
    """Récupère tous les cinémas de Paris depuis l'API."""
    api = allocineAPI()
    
    logger.info("Récupération des cinémas parisiens...")
    try:
        cinemas = api.get_cinema(PARIS_LOCATION_ID)
        logger.info(f"Trouvé {len(cinemas)} cinémas à Paris")
        return cinemas
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des cinémas: {e}")
        return []


def import_cinemas(cinemas: List[dict]) -> Set[str]:
    """
    Importe les cinémas dans la base de données en BULK.
    Retourne les IDs des cinémas importés avec succès.
    """
    from db.insert_logic import cinema_id_to_int
    import re
    
    cinema_ids = set()
    cinemas_to_insert = []
    
    # Préparer tous les cinémas
    for cinema in cinemas:
        try:
            cinema_id = cinema.get("id")
            if not cinema_id:
                continue
                
            # Enrichir les données avec ville/code postal
            cinema["city"] = "Paris"
            if not cinema.get("zipcode"):
                # Essayer d'extraire le code postal de l'adresse
                address = cinema.get("address", "")
                cp_match = re.search(r'\b75\d{3}\b', address)
                cinema["zipcode"] = cp_match.group() if cp_match else "75000"
            
            # Préparer pour l'insertion
            cinemas_to_insert.append({
                "id": cinema_id_to_int(cinema_id),
                "name": cinema["name"],
                "address": cinema.get("address"),
                "city": cinema["city"],
                "zipcode": cinema["zipcode"]
            })
            cinema_ids.add(cinema_id)
            
        except Exception as e:
            logger.error(f"Erreur préparation cinéma {cinema.get('name')}: {e}")
    
    # Bulk insert
    try:
        if cinemas_to_insert:
            # Utiliser upsert pour éviter les erreurs de duplication
            result = supabase.table("cinemas").upsert(
                cinemas_to_insert, 
                on_conflict="id"
            ).execute()
            logger.info(f"Bulk insert de {len(cinemas_to_insert)} cinémas effectué")
    except Exception as e:
        logger.error(f"Erreur lors du bulk insert des cinémas: {e}")
        # En cas d'erreur, essayer un par un
        for cinema_data in cinemas_to_insert:
            try:
                supabase.table("cinemas").upsert(cinema_data).execute()
            except:
                cinema_ids.discard(next(c["id"] for c in cinemas if cinema_id_to_int(c["id"]) == cinema_data["id"]))
    
    logger.info(f"Importé {len(cinema_ids)} cinémas avec succès")
    return cinema_ids


def import_screenings_with_retry(cinema_id: str, date: str, max_retries: int = MAX_RETRIES) -> tuple:
    """Importe les séances avec retry en cas d'échec."""
    for attempt in range(max_retries):
        try:
            movies, screenings = process_cinema_screenings(cinema_id, date)
            return movies, screenings
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1}/{max_retries} échouée pour {cinema_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(DELAY_BETWEEN_REQUESTS * (attempt + 1))  # Backoff
            else:
                logger.error(f"Échec définitif pour {cinema_id} le {date}")
                return 0, 0


def clean_old_screenings(days_to_keep: int = 30):
    """Supprime les séances de plus de X jours."""
    cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
    
    try:
        result = supabase.table("screenings").delete().lt("date", cutoff_date).execute()
        logger.info(f"Nettoyage: supprimé les séances avant le {cutoff_date}")
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage: {e}")


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(description="Import des données cinéma pour Paris")
    parser.add_argument("--days", type=int, default=7, help="Nombre de jours à importer (défaut: 7)")
    parser.add_argument("--test", action="store_true", help="Mode test: importe seulement 3 cinémas")
    parser.add_argument("--clean", action="store_true", help="Nettoyer les vieilles séances avant import")
    parser.add_argument("--cinema", type=str, help="Importer un seul cinéma par son ID")
    
    args = parser.parse_args()
    
    logger.info(f"=== Début import Paris - {args.days} jours ===")
    start_time = datetime.now()
    
    # Nettoyage optionnel
    if args.clean:
        clean_old_screenings()
    
    # Si un cinéma spécifique est demandé
    if args.cinema:
        cinema_ids = {args.cinema}
        logger.info(f"Mode cinéma unique: {args.cinema}")
    else:
        # Récupérer et importer les cinémas
        cinemas = get_paris_cinemas()
        if not cinemas:
            logger.error("Aucun cinéma trouvé, abandon")
            return 1
        
        # En mode test, limiter à 3 cinémas
        if args.test:
            cinemas = cinemas[:3]
            logger.info("Mode test: limitation à 3 cinémas")
        
        cinema_ids = import_cinemas(cinemas)
    
    # Générer les dates
    dates = [(datetime.today() + timedelta(days=i)).strftime("%Y-%m-%d") 
             for i in range(args.days)]
    
    # Stats globales
    total_movies = 0
    total_screenings = 0
    failed_imports = []
    
    # Importer les séances
    total_operations = len(cinema_ids) * len(dates)
    current_op = 0
    
    for cinema_id in cinema_ids:
        for date in dates:
            current_op += 1
            logger.info(f"Import {current_op}/{total_operations}: Cinéma {cinema_id} pour le {date}")
            
            movies, screenings = import_screenings_with_retry(cinema_id, date)
            total_movies += movies
            total_screenings += screenings
            
            if movies == 0 and screenings == 0:
                failed_imports.append((cinema_id, date))
            
            # Pause entre les requêtes
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Rapport final
    duration = datetime.now() - start_time
    logger.info("=== RAPPORT FINAL ===")
    logger.info(f"Durée totale: {duration}")
    logger.info(f"Cinémas traités: {len(cinema_ids)}")
    logger.info(f"Jours importés: {args.days}")
    logger.info(f"Films uniques ajoutés: {total_movies}")
    logger.info(f"Séances ajoutées: {total_screenings}")
    
    if failed_imports:
        logger.warning(f"Échecs d'import: {len(failed_imports)}")
        for cinema_id, date in failed_imports[:10]:  # Limiter l'affichage
            logger.warning(f"  - {cinema_id} le {date}")
    
    # Stats moyennes
    if cinema_ids and args.days > 0:
        avg_screenings = total_screenings / (len(cinema_ids) * args.days)
        logger.info(f"Moyenne séances/cinéma/jour: {avg_screenings:.1f}")
    
    return 0 if not failed_imports else 1


if __name__ == "__main__":
    sys.exit(main())