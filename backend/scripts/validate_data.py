#!/usr/bin/env python3
"""
Script de validation de la qualité des données pour 35mm-paris.
Vérifie les doublons, les incohérences et génère un rapport.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Add src to path
sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.supabase_client import supabase
from utils.logger import get_logger

logger = get_logger(__name__)


class DataValidator:
    """Validateur de données pour la base 35mm-paris."""
    
    def __init__(self):
        self.issues = []
        self.stats = {}
    
    def add_issue(self, category: str, description: str, severity: str = "WARNING"):
        """Ajoute un problème détecté."""
        self.issues.append({
            "category": category,
            "description": description,
            "severity": severity,
            "timestamp": datetime.now().isoformat()
        })
    
    def check_duplicate_movies(self) -> Dict[str, int]:
        """Vérifie les films en doublon (même titre et durée)."""
        logger.info("Vérification des films en doublon...")
        
        # Récupérer tous les films
        movies = supabase.table("movies").select("id, title, original_title, runtime").execute()
        
        # Grouper par titre + durée
        movie_groups = {}
        for movie in movies.data:
            key = f"{movie['title'].lower()}_{movie['runtime']}"
            if key not in movie_groups:
                movie_groups[key] = []
            movie_groups[key].append(movie)
        
        # Identifier les doublons
        duplicates = {k: v for k, v in movie_groups.items() if len(v) > 1}
        
        if duplicates:
            for key, movies_list in duplicates.items():
                self.add_issue(
                    "DUPLICATE_MOVIES",
                    f"Film '{movies_list[0]['title']}' ({movies_list[0]['runtime']}min) "
                    f"existe {len(movies_list)} fois avec IDs: "
                    f"{[m['id'] for m in movies_list]}",
                    "ERROR"
                )
        
        self.stats["total_movies"] = len(movies.data)
        self.stats["duplicate_movies"] = sum(len(v) - 1 for v in duplicates.values())
        
        return duplicates
    
    def check_duplicate_screenings(self) -> int:
        """Vérifie les séances en doublon."""
        logger.info("Vérification des séances en doublon...")
        
        # Requête pour trouver les doublons
        query = """
        SELECT movie_id, cinema_id, date, starts_at, COUNT(*) as count
        FROM screenings
        GROUP BY movie_id, cinema_id, date, starts_at
        HAVING COUNT(*) > 1
        """
        
        # Supabase ne supporte pas les requêtes SQL brutes facilement
        # On va faire une vérification par échantillonnage
        today = datetime.now().strftime("%Y-%m-%d")
        screenings = supabase.table("screenings").select("*").eq("date", today).execute()
        
        seen = set()
        duplicates = 0
        
        for screening in screenings.data:
            key = (
                screening['movie_id'],
                screening['cinema_id'],
                screening['date'],
                screening['starts_at']
            )
            if key in seen:
                duplicates += 1
                self.add_issue(
                    "DUPLICATE_SCREENING",
                    f"Séance en doublon: Film {screening['movie_id']} "
                    f"au cinéma {screening['cinema_id']} "
                    f"le {screening['date']} à {screening['starts_at']}",
                    "ERROR"
                )
            seen.add(key)
        
        self.stats["duplicate_screenings_sample"] = duplicates
        return duplicates
    
    def check_orphaned_data(self):
        """Vérifie les données orphelines."""
        logger.info("Vérification des données orphelines...")
        
        # Récupérer TOUS les IDs uniques en paginant
        all_cinema_ids_with_screenings = set()
        all_movie_ids = set()
        
        # Paginer pour récupérer toutes les séances
        page = 0
        while True:
            offset = page * 1000
            try:
                batch = supabase.table("screenings").select("movie_id, cinema_id").range(offset, offset + 999).execute()
                if not batch.data:
                    break
                for s in batch.data:
                    all_movie_ids.add(s['movie_id'])
                    all_cinema_ids_with_screenings.add(s['cinema_id'])
                if len(batch.data) < 1000:
                    break
                page += 1
            except Exception as e:
                logger.error(f"Erreur pagination: {e}")
                break
        
        movie_ids = all_movie_ids
        cinema_ids = all_cinema_ids_with_screenings
        
        movies = supabase.table("movies").select("id").execute()
        valid_movie_ids = {m['id'] for m in movies.data}
        
        orphaned_movie_refs = movie_ids - valid_movie_ids
        if orphaned_movie_refs:
            self.add_issue(
                "ORPHANED_SCREENINGS",
                f"{len(orphaned_movie_refs)} séances référencent des films inexistants",
                "ERROR"
            )
        
        # Cinémas sans séances
        cinemas = supabase.table("cinemas").select("id").execute()
        all_cinema_ids = {c['id'] for c in cinemas.data}
        unused_cinemas = all_cinema_ids - cinema_ids
        
        if unused_cinemas:
            self.add_issue(
                "UNUSED_CINEMAS",
                f"{len(unused_cinemas)} cinémas n'ont aucune séance",
                "WARNING"
            )
        
        self.stats["orphaned_movie_refs"] = len(orphaned_movie_refs)
        self.stats["unused_cinemas"] = len(unused_cinemas)
    
    def check_data_consistency(self):
        """Vérifie la cohérence des données."""
        logger.info("Vérification de la cohérence des données...")
        
        # Films sans réalisateur
        movies = supabase.table("movies").select("id, title").execute()
        movie_directors = supabase.table("movie_directors").select("movie_id").execute()
        movies_with_directors = {md['movie_id'] for md in movie_directors.data}
        
        movies_without_directors = []
        for movie in movies.data:
            if movie['id'] not in movies_with_directors:
                movies_without_directors.append(movie)
        
        if movies_without_directors:
            self.add_issue(
                "MISSING_DIRECTORS",
                f"{len(movies_without_directors)} films sans réalisateur",
                "INFO"
            )
        
        # Séances dans le passé
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        old_screenings = supabase.table("screenings").select("count", count="exact").lt("date", yesterday).execute()
        
        if old_screenings.count > 0:
            self.add_issue(
                "OLD_SCREENINGS",
                f"{old_screenings.count} séances dans le passé (avant {yesterday})",
                "INFO"
            )
        
        self.stats["movies_without_directors"] = len(movies_without_directors)
        self.stats["old_screenings"] = old_screenings.count
    
    def check_data_completeness(self):
        """Vérifie la complétude des données."""
        logger.info("Vérification de la complétude des données...")
        
        # Films sans synopsis
        movies_no_synopsis = supabase.table("movies").select("count", count="exact").is_("synopsis", "null").execute()
        
        # Films sans poster
        movies_no_poster = supabase.table("movies").select("count", count="exact").is_("poster_url", "null").execute()
        
        # Cinémas sans adresse complète
        cinemas_no_address = supabase.table("cinemas").select("count", count="exact").is_("address", "null").execute()
        cinemas_no_zipcode = supabase.table("cinemas").select("count", count="exact").is_("zipcode", "null").execute()
        
        if movies_no_synopsis.count > 0:
            self.add_issue(
                "MISSING_SYNOPSIS",
                f"{movies_no_synopsis.count} films sans synopsis",
                "INFO"
            )
        
        if movies_no_poster.count > 0:
            self.add_issue(
                "MISSING_POSTER",
                f"{movies_no_poster.count} films sans affiche",
                "INFO"
            )
        
        self.stats["movies_no_synopsis"] = movies_no_synopsis.count
        self.stats["movies_no_poster"] = movies_no_poster.count
        self.stats["cinemas_no_address"] = cinemas_no_address.count
        self.stats["cinemas_no_zipcode"] = cinemas_no_zipcode.count
    
    def generate_report(self) -> str:
        """Génère un rapport de validation."""
        report = []
        report.append("=" * 60)
        report.append("RAPPORT DE VALIDATION DES DONNÉES - 35mm Paris")
        report.append("=" * 60)
        report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Statistiques
        report.append("STATISTIQUES")
        report.append("-" * 30)
        for key, value in sorted(self.stats.items()):
            report.append(f"{key:.<40} {value:>10}")
        report.append("")
        
        # Problèmes par sévérité
        errors = [i for i in self.issues if i['severity'] == 'ERROR']
        warnings = [i for i in self.issues if i['severity'] == 'WARNING']
        infos = [i for i in self.issues if i['severity'] == 'INFO']
        
        if errors:
            report.append(f"ERREURS ({len(errors)})")
            report.append("-" * 30)
            for issue in errors:
                report.append(f"[{issue['category']}] {issue['description']}")
            report.append("")
        
        if warnings:
            report.append(f"AVERTISSEMENTS ({len(warnings)})")
            report.append("-" * 30)
            for issue in warnings:
                report.append(f"[{issue['category']}] {issue['description']}")
            report.append("")
        
        if infos:
            report.append(f"INFORMATIONS ({len(infos)})")
            report.append("-" * 30)
            for issue in infos[:10]:  # Limiter à 10 pour ne pas surcharger
                report.append(f"[{issue['category']}] {issue['description']}")
            if len(infos) > 10:
                report.append(f"... et {len(infos) - 10} autres")
            report.append("")
        
        # Résumé
        report.append("RÉSUMÉ")
        report.append("-" * 30)
        if errors:
            report.append(f"❌ {len(errors)} erreurs critiques détectées")
        else:
            report.append("✅ Aucune erreur critique")
        
        if warnings:
            report.append(f"⚠️  {len(warnings)} avertissements")
        
        total_issues = len(self.issues)
        if total_issues == 0:
            report.append("✅ Toutes les validations sont passées avec succès!")
        else:
            report.append(f"Total: {total_issues} problèmes détectés")
        
        return "\n".join(report)


def main():
    """Point d'entrée principal."""
    logger.info("Démarrage de la validation des données...")
    
    validator = DataValidator()
    
    try:
        # Exécuter toutes les validations
        validator.check_duplicate_movies()
        validator.check_duplicate_screenings()
        validator.check_orphaned_data()
        validator.check_data_consistency()
        validator.check_data_completeness()
        
        # Générer et afficher le rapport
        report = validator.generate_report()
        print(report)
        
        # Sauvegarder le rapport
        report_path = Path("validation_report.txt")
        report_path.write_text(report)
        logger.info(f"Rapport sauvegardé dans {report_path}")
        
        # Code de sortie basé sur les erreurs
        errors = [i for i in validator.issues if i['severity'] == 'ERROR']
        return 1 if errors else 0
        
    except Exception as e:
        logger.error(f"Erreur lors de la validation: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())