#!/usr/bin/env python3
"""
Script de validation amélioré pour 35mm-paris.
Vérifie les doublons, les incohérences et génère un rapport détaillé.
"""
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

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
        self.issues.append(
            {
                "category": category,
                "description": description,
                "severity": severity,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def check_duplicate_movies(self) -> dict[str, list]:
        """Vérifie les films en doublon (même titre et durée)."""
        logger.info("Vérification des films en doublon...")

        # Récupérer tous les films
        movies = (
            supabase.table("movies")
            .select("id, title, original_title, runtime")
            .execute()
        )

        # Grouper par titre normalisé + durée
        movie_groups = defaultdict(list)
        for movie in movies.data:
            # Normaliser le titre pour détecter les variations
            normalized_title = movie["title"].lower().strip()
            key = f"{normalized_title}_{movie['runtime']}"
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
                    "ERROR",
                )

        self.stats["total_movies"] = len(movies.data)
        self.stats["duplicate_movies"] = sum(len(v) - 1 for v in duplicates.values())

        return duplicates

    def check_duplicate_screenings(self) -> int:
        """Vérifie les séances en doublon sur plusieurs jours."""
        logger.info("Vérification des séances en doublon...")

        # Vérifier sur les 7 prochains jours
        duplicate_count = 0
        today = datetime.now()

        for days_offset in range(7):
            check_date = (today + timedelta(days=days_offset)).strftime("%Y-%m-%d")

            try:
                screenings = (
                    supabase.table("screenings")
                    .select("movie_id, cinema_id, date, starts_at")
                    .eq("date", check_date)
                    .execute()
                )

                seen = set()
                for screening in screenings.data:
                    key = (
                        screening["movie_id"],
                        screening["cinema_id"],
                        screening["date"],
                        screening["starts_at"],
                    )
                    if key in seen:
                        duplicate_count += 1
                        if duplicate_count <= 5:  # Limiter les logs
                            self.add_issue(
                                "DUPLICATE_SCREENING",
                                f"Séance en doublon: Film {screening['movie_id']} "
                                f"au cinéma {screening['cinema_id']} "
                                f"le {screening['date']} à {screening['starts_at']}",
                                "ERROR",
                            )
                    seen.add(key)

            except Exception as e:
                logger.error(f"Erreur vérification doublons pour {check_date}: {e}")

        if duplicate_count > 5:
            self.add_issue(
                "DUPLICATE_SCREENING",
                f"... et {duplicate_count - 5} autres séances en doublon",
                "ERROR",
            )

        self.stats["duplicate_screenings"] = duplicate_count
        return duplicate_count

    def check_orphaned_data(self):
        """Vérifie les données orphelines."""
        logger.info("Vérification des données orphelines...")
        
        # 1. Récupérer tous les cinémas
        all_cinemas = supabase.table("cinemas").select("id, name").execute()
        all_cinema_ids = {c['id'] for c in all_cinemas.data}
        cinema_names = {c['id']: c['name'] for c in all_cinemas.data}
        
        # 2. Récupérer les cinémas qui ont au moins une séance
        # Utiliser une requête plus efficace avec distinct
        cinemas_with_screenings = set()
        
        # Paginer pour récupérer tous les cinema_ids uniques
        page = 0
        while True:
            offset = page * 1000
            try:
                # Select uniquement cinema_id pour optimiser
                batch = supabase.table("screenings").select("cinema_id").range(offset, offset + 999).execute()
                if not batch.data:
                    break
                
                # Ajouter les cinema_ids au set
                for screening in batch.data:
                    cinemas_with_screenings.add(screening['cinema_id'])
                
                if len(batch.data) < 1000:
                    break
                page += 1
                
            except Exception as e:
                logger.error(f"Erreur pagination: {e}")
                break
        
        # 3. Calculer les cinémas sans séances
        unused_cinemas = all_cinema_ids - cinemas_with_screenings
        
        if unused_cinemas:
            # Log les noms des cinémas pour debug
            unused_names = [cinema_names.get(cid, f"ID: {cid}") for cid in list(unused_cinemas)[:5]]
            logger.info(f"Exemples de cinémas sans séances: {unused_names}")
            
            self.add_issue(
                "UNUSED_CINEMAS",
                f"{len(unused_cinemas)} cinémas n'ont aucune séance",
                "WARNING"
            )
        
        # 4. Vérifier les références de films orphelines
        all_movie_ids = set()
        
        # Récupérer tous les movie_ids référencés dans les séances
        page = 0
        while True:
            offset = page * 1000
            try:
                batch = supabase.table("screenings").select("movie_id").range(offset, offset + 999).execute()
                if not batch.data:
                    break
                
                for screening in batch.data:
                    all_movie_ids.add(screening['movie_id'])
                
                if len(batch.data) < 1000:
                    break
                page += 1
                
            except Exception as e:
                logger.error(f"Erreur pagination films: {e}")
                break
        
        # Récupérer tous les films existants
        movies = supabase.table("movies").select("id").execute()
        valid_movie_ids = {m['id'] for m in movies.data}
        
        orphaned_movie_refs = all_movie_ids - valid_movie_ids
        if orphaned_movie_refs:
            self.add_issue(
                "ORPHANED_SCREENINGS",
                f"{len(orphaned_movie_refs)} séances référencent des films inexistants",
                "ERROR"
            )
        
        # Mettre à jour les stats
        self.stats["orphaned_movie_refs"] = len(orphaned_movie_refs)
        self.stats["unused_cinemas"] = len(unused_cinemas)
        self.stats["total_cinemas"] = len(all_cinema_ids)
        self.stats["cinemas_with_screenings"] = len(cinemas_with_screenings)
        
        logger.info(f"Stats cinémas: {len(all_cinema_ids)} total, "
                    f"{len(cinemas_with_screenings)} avec séances, "
                    f"{len(unused_cinemas)} sans séances")

    def check_data_consistency(self):
        """Vérifie la cohérence des données."""
        logger.info("Vérification de la cohérence des données...")

        # Films sans réalisateur
        movies = supabase.table("movies").select("id, title").execute()
        movie_directors = supabase.table("movie_directors").select("movie_id").execute()
        movies_with_directors = {md["movie_id"] for md in movie_directors.data}

        movies_without_directors = []
        for movie in movies.data:
            if movie["id"] not in movies_with_directors:
                movies_without_directors.append(movie)

        if movies_without_directors:
            self.add_issue(
                "MISSING_DIRECTORS",
                f"{len(movies_without_directors)} films sans réalisateur "
                f"(ex: {movies_without_directors[0]['title']})",
                "INFO",
            )

        # Séances dans le passé
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        old_screenings = (
            supabase.table("screenings")
            .select("count", count="exact")
            .lt("date", yesterday)
            .execute()
        )

        if old_screenings.count > 100:  # Seuil d'alerte
            self.add_issue(
                "OLD_SCREENINGS",
                f"{old_screenings.count} séances dans le passé - "
                f"considérer un nettoyage avec --clean",
                "WARNING",
            )

        # Séances trop loin dans le futur (>30 jours)
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        future_screenings = (
            supabase.table("screenings")
            .select("count", count="exact")
            .gt("date", future_date)
            .execute()
        )

        if future_screenings.count > 0:
            self.add_issue(
                "FAR_FUTURE_SCREENINGS",
                f"{future_screenings.count} séances programmées à plus de 30 jours",
                "INFO",
            )

        self.stats["movies_without_directors"] = len(movies_without_directors)
        self.stats["old_screenings"] = old_screenings.count
        self.stats["future_screenings"] = future_screenings.count

    def check_circuits_consistency(self):
        """Vérifie la cohérence des circuits."""
        logger.info("Vérification des circuits...")

        # Récupérer tous les circuits
        circuits = supabase.table("circuits").select("id, code, name").execute()
        circuit_ids = {c["id"] for c in circuits.data}

        # Vérifier les cinémas avec circuit_id invalide
        cinemas = (
            supabase.table("cinemas")
            .select("id, name, circuit_id")
            .not_.is_("circuit_id", "null")
            .execute()
        )

        invalid_circuit_refs = []
        for cinema in cinemas.data:
            if cinema["circuit_id"] not in circuit_ids:
                invalid_circuit_refs.append(cinema)

        if invalid_circuit_refs:
            self.add_issue(
                "INVALID_CIRCUIT_REFS",
                f"{len(invalid_circuit_refs)} cinémas avec circuit_id invalide",
                "ERROR",
            )

        # Circuits sans cinémas
        cinema_circuits = {c["circuit_id"] for c in cinemas.data if c["circuit_id"]}
        empty_circuits = circuit_ids - cinema_circuits

        if empty_circuits:
            empty_names = [
                c["name"] for c in circuits.data if c["id"] in empty_circuits
            ]
            self.add_issue(
                "EMPTY_CIRCUITS",
                f"{len(empty_circuits)} circuits sans cinémas: {', '.join(empty_names[:3])}...",
                "INFO",
            )

        self.stats["total_circuits"] = len(circuits.data)
        self.stats["invalid_circuit_refs"] = len(invalid_circuit_refs)
        self.stats["empty_circuits"] = len(empty_circuits)

    def check_data_completeness(self):
        """Vérifie la complétude des données."""
        logger.info("Vérification de la complétude des données...")

        # Films sans synopsis
        movies_no_synopsis = (
            supabase.table("movies")
            .select("count", count="exact")
            .is_("synopsis", "null")
            .execute()
        )

        # Films sans poster
        movies_no_poster = (
            supabase.table("movies")
            .select("count", count="exact")
            .is_("poster_url", "null")
            .execute()
        )

        # Films sans langue
        movies_count = supabase.table("movies").select("count", count="exact").execute()
        movie_languages_count = (
            supabase.table("movie_languages")
            .select("movie_id", count="exact")
            .execute()
        )

        # Approximation des films sans langue (pas parfait mais suffisant)
        approx_movies_no_language = max(
            0, movies_count.count - movie_languages_count.count // 2
        )

        # Cinémas sans adresse complète
        cinemas_no_address = (
            supabase.table("cinemas")
            .select("count", count="exact")
            .is_("address", "null")
            .execute()
        )

        cinemas_no_zipcode = (
            supabase.table("cinemas")
            .select("count", count="exact")
            .is_("zipcode", "null")
            .execute()
        )

        # Générer les issues appropriées
        if movies_no_synopsis.count > 10:
            self.add_issue(
                "MISSING_SYNOPSIS",
                f"{movies_no_synopsis.count} films sans synopsis",
                "INFO",
            )

        if movies_no_poster.count > 10:
            self.add_issue(
                "MISSING_POSTER", f"{movies_no_poster.count} films sans affiche", "INFO"
            )

        if approx_movies_no_language > 10:
            self.add_issue(
                "MISSING_LANGUAGES",
                f"Environ {approx_movies_no_language} films sans information de langue",
                "INFO",
            )

        self.stats["movies_no_synopsis"] = movies_no_synopsis.count
        self.stats["movies_no_poster"] = movies_no_poster.count
        self.stats["movies_no_language_approx"] = approx_movies_no_language
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
            # Formatter le nom de la stat pour plus de lisibilité
            formatted_key = key.replace("_", " ").title()
            report.append(f"{formatted_key:.<40} {value:>10}")
        report.append("")

        # Problèmes par sévérité
        errors = [i for i in self.issues if i["severity"] == "ERROR"]
        warnings = [i for i in self.issues if i["severity"] == "WARNING"]
        infos = [i for i in self.issues if i["severity"] == "INFO"]

        if errors:
            report.append(f"❌ ERREURS ({len(errors)})")
            report.append("-" * 30)
            for issue in errors:
                report.append(f"[{issue['category']}] {issue['description']}")
            report.append("")

        if warnings:
            report.append(f"⚠️  AVERTISSEMENTS ({len(warnings)})")
            report.append("-" * 30)
            for issue in warnings:
                report.append(f"[{issue['category']}] {issue['description']}")
            report.append("")

        if infos:
            report.append(f"ℹ️  INFORMATIONS ({len(infos)})")
            report.append("-" * 30)
            for issue in infos[:10]:  # Limiter à 10 pour ne pas surcharger
                report.append(f"[{issue['category']}] {issue['description']}")
            if len(infos) > 10:
                report.append(f"... et {len(infos) - 10} autres")
            report.append("")

        # Résumé et recommandations
        report.append("RÉSUMÉ ET RECOMMANDATIONS")
        report.append("-" * 30)

        if errors:
            report.append(
                f"❌ {len(errors)} erreurs critiques détectées nécessitant une action"
            )
            if any("DUPLICATE" in i["category"] for i in errors):
                report.append("   → Exécuter un script de déduplication")
            if any("ORPHANED" in i["category"] for i in errors):
                report.append("   → Nettoyer les références invalides")
        else:
            report.append("✅ Aucune erreur critique")

        if warnings:
            report.append(f"⚠️  {len(warnings)} avertissements à surveiller")
            if self.stats.get("old_screenings", 0) > 100:
                report.append(
                    "   → Lancer 'python import_paris.py --clean' pour nettoyer les vieilles séances"
                )
            if self.stats.get("inactive_cinemas", 0) > 20:
                report.append(
                    "   → Vérifier si certains cinémas sont fermés définitivement"
                )

        total_issues = len(self.issues)
        if total_issues == 0:
            report.append("✅ Toutes les validations sont passées avec succès!")
        else:
            report.append(f"\nTotal: {total_issues} problèmes détectés")

        # Score de qualité
        quality_score = self._calculate_quality_score()
        report.append(f"\n📊 Score de qualité des données: {quality_score}%")

        return "\n".join(report)

    def _calculate_quality_score(self) -> int:
        """Calcule un score de qualité global des données."""
        # Commencer à 100 et déduire des points
        score = 100

        # Déductions pour erreurs critiques
        errors = [i for i in self.issues if i["severity"] == "ERROR"]
        score -= len(errors) * 5  # -5 points par erreur

        # Déductions pour données manquantes
        total_movies = self.stats.get("total_movies", 1)
        if total_movies > 0:
            missing_synopsis_ratio = (
                self.stats.get("movies_no_synopsis", 0) / total_movies
            )
            missing_poster_ratio = self.stats.get("movies_no_poster", 0) / total_movies
            score -= int(missing_synopsis_ratio * 10)  # Max -10 points
            score -= int(missing_poster_ratio * 10)  # Max -10 points

        # Déductions pour incohérences
        if self.stats.get("duplicate_movies", 0) > 0:
            score -= 10
        if self.stats.get("duplicate_screenings", 0) > 0:
            score -= 5

        return max(0, score)  # Ne pas descendre en dessous de 0


def main():
    """Point d'entrée principal."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Valide la qualité des données de 35mm-paris"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Validation rapide (skip certains checks lourds)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="validation_report.txt",
        help="Fichier de sortie pour le rapport (défaut: validation_report.txt)",
    )

    args = parser.parse_args()

    logger.info("Démarrage de la validation des données...")

    validator = DataValidator()

    try:
        # Exécuter toutes les validations
        validator.check_duplicate_movies()

        if not args.quick:
            validator.check_duplicate_screenings()

        validator.check_orphaned_data()
        validator.check_data_consistency()
        validator.check_circuits_consistency()
        validator.check_data_completeness()

        # Générer et afficher le rapport
        report = validator.generate_report()
        print("\n" + report)

        # Sauvegarder le rapport
        report_path = Path(args.output)
        report_path.write_text(report)
        logger.info(f"Rapport sauvegardé dans {report_path}")

        # Code de sortie basé sur les erreurs
        errors = [i for i in validator.issues if i["severity"] == "ERROR"]
        return 1 if errors else 0

    except Exception as e:
        logger.error(f"Erreur lors de la validation: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
