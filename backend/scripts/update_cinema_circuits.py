#!/usr/bin/env python3
"""
Script optimisé pour créer les circuits et les associer aux cinémas.
Utilise les fonctions existantes de insert_logic pour éviter la duplication.
Usage: python update_cinema_circuits.py [--dry-run]
"""
import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from allocineAPI.allocineAPI import allocineAPI

from db.insert_logic import cinema_id_to_int, generate_circuit_id
from db.supabase_client import supabase
from utils.logger import get_logger

logger = get_logger(__name__)

DELAY_BETWEEN_REQUESTS = 0.5
BATCH_SIZE = 100


def fetch_all_circuits() -> dict[str, dict]:
    """
    Récupère tous les circuits depuis l'API.

    Returns:
        Dict mapping circuit_code -> {id, name, code}
    """
    api = allocineAPI()
    circuits_map = {}

    try:
        circuits_data = api.get_circuit()
        logger.info(f"Trouvé {len(circuits_data)} circuits")

        for circuit in circuits_data:
            circuit_code = circuit["id"]
            circuit_id = generate_circuit_id(circuit_code)

            circuits_map[circuit_code] = {
                "id": circuit_id,
                "code": circuit_code,
                "name": circuit["name"],
            }

        return circuits_map

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des circuits: {e}")
        return {}


def insert_circuits_to_db(circuits_map: dict[str, dict]) -> bool:
    """
    Insère tous les circuits dans la base de données.

    Args:
        circuits_map: Dict des circuits à insérer

    Returns:
        True si succès, False sinon
    """
    if not circuits_map:
        return False

    try:
        circuits_to_insert = list(circuits_map.values())

        # Bulk upsert
        (
            supabase.table("circuits")
            .upsert(circuits_to_insert, on_conflict="id")
            .execute()
        )

        logger.info(f"✅ {len(circuits_to_insert)} circuits insérés/mis à jour")
        return True

    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des circuits: {e}")
        return False


def map_cinemas_to_circuits(circuits_map: dict[str, dict]) -> dict[int, int]:
    """
    Crée un mapping cinema_id -> circuit_id en parcourant tous les circuits.

    Args:
        circuits_map: Dict des circuits

    Returns:
        Dict mapping cinema_id (int) -> circuit_id
    """
    api = allocineAPI()
    cinema_to_circuit = {}

    for circuit_code, circuit_info in circuits_map.items():
        circuit_id = circuit_info["id"]
        circuit_name = circuit_info["name"]

        logger.info(f"Traitement du circuit: {circuit_name}")

        try:
            # Récupérer les cinémas de ce circuit
            cinemas = api.get_cinema(circuit_code)
            logger.info(f"  → {len(cinemas)} cinémas trouvés")

            for cinema in cinemas:
                cinema_id_str = cinema["id"]
                cinema_id_int = cinema_id_to_int(cinema_id_str)
                cinema_to_circuit[cinema_id_int] = circuit_id

            time.sleep(DELAY_BETWEEN_REQUESTS)

        except Exception as e:
            logger.error(f"Erreur pour le circuit {circuit_name}: {e}")
            continue

    return cinema_to_circuit


def update_cinemas_circuits(
    cinema_to_circuit: dict[int, int], dry_run: bool = False
) -> int:
    """
    Met à jour les circuit_id des cinémas.

    Args:
        cinema_to_circuit: Mapping cinema_id -> circuit_id
        dry_run: Si True, ne fait pas les updates

    Returns:
        Nombre de cinémas mis à jour
    """
    if not cinema_to_circuit:
        return 0

    total_updates = 0

    # Si dry run, juste afficher ce qui serait fait
    if dry_run:
        logger.info("🔍 Mode DRY RUN - Aucune modification ne sera faite")
        for cinema_id, circuit_id in list(cinema_to_circuit.items())[:10]:
            logger.info(f"  Cinema {cinema_id} → Circuit {circuit_id}")
        if len(cinema_to_circuit) > 10:
            logger.info(f"  ... et {len(cinema_to_circuit) - 10} autres")
        return len(cinema_to_circuit)

    # Grouper par circuit_id pour des updates plus efficaces
    circuit_to_cinemas = defaultdict(list)
    for cinema_id, circuit_id in cinema_to_circuit.items():
        circuit_to_cinemas[circuit_id].append(cinema_id)

    # Faire les updates par circuit
    for circuit_id, cinema_ids in circuit_to_cinemas.items():
        logger.info(
            f"Mise à jour de {len(cinema_ids)} cinémas pour le circuit {circuit_id}"
        )

        # Update par batch
        for i in range(0, len(cinema_ids), BATCH_SIZE):
            batch = cinema_ids[i : i + BATCH_SIZE]

            try:
                result = (
                    supabase.table("cinemas")
                    .update({"circuit_id": circuit_id})
                    .in_("id", batch)
                    .execute()
                )

                total_updates += len(result.data)

            except Exception as e:
                logger.error(f"Erreur batch update: {e}")

    return total_updates


def generate_statistics() -> None:
    """Génère et affiche les statistiques finales."""
    logger.info("\n=== STATISTIQUES ===")

    # Circuits avec leurs cinémas
    try:
        circuits = supabase.table("circuits").select("*").execute()

        circuit_stats = []
        for circuit in circuits.data:
            count_result = (
                supabase.table("cinemas")
                .select("id", count="exact")
                .eq("circuit_id", circuit["id"])
                .execute()
            )

            if count_result.count > 0:
                circuit_stats.append(
                    {"name": circuit["name"], "count": count_result.count}
                )

        # Trier par nombre de cinémas
        circuit_stats.sort(key=lambda x: x["count"], reverse=True)

        logger.info("\nCinémas par circuit:")
        for stat in circuit_stats:
            logger.info(f"  - {stat['name']}: {stat['count']} cinémas")

        # Cinémas indépendants
        independents = (
            supabase.table("cinemas")
            .select("id, name")
            .is_("circuit_id", "null")
            .execute()
        )

        if independents.data:
            logger.info(f"\nCinémas indépendants: {len(independents.data)}")
            for cinema in independents.data[:5]:
                logger.info(f"  - {cinema['name']}")
            if len(independents.data) > 5:
                logger.info(f"  ... et {len(independents.data) - 5} autres")

    except Exception as e:
        logger.error(f"Erreur lors de la génération des statistiques: {e}")


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(
        description="Met à jour les circuits et leurs associations avec les cinémas"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode simulation - affiche ce qui serait fait sans modifier la base",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Affiche seulement les statistiques sans faire de modifications",
    )

    args = parser.parse_args()

    if args.stats_only:
        generate_statistics()
        return 0

    logger.info("=== Mise à jour des circuits et associations ===")

    # Étape 1: Récupérer tous les circuits
    logger.info("\n📥 ÉTAPE 1: Récupération des circuits")
    circuits_map = fetch_all_circuits()

    if not circuits_map:
        logger.error("Aucun circuit trouvé, arrêt")
        return 1

    # Étape 2: Insérer les circuits dans la base
    if not args.dry_run:
        logger.info("\n💾 ÉTAPE 2: Insertion des circuits")
        if not insert_circuits_to_db(circuits_map):
            logger.error("Échec de l'insertion des circuits")
            return 1

    # Étape 3: Mapper les cinémas aux circuits
    logger.info("\n🔗 ÉTAPE 3: Mapping cinémas-circuits")
    cinema_to_circuit = map_cinemas_to_circuits(circuits_map)

    logger.info(f"Trouvé {len(cinema_to_circuit)} associations cinéma-circuit")

    # Étape 4: Mettre à jour les cinémas
    logger.info("\n🔄 ÉTAPE 4: Mise à jour des cinémas")
    updates = update_cinemas_circuits(cinema_to_circuit, dry_run=args.dry_run)

    logger.info(
        f"\n✅ {updates} cinémas {'seraient' if args.dry_run else 'ont été'} mis à jour"
    )

    # Étape 5: Afficher les statistiques
    if not args.dry_run:
        generate_statistics()

    return 0


if __name__ == "__main__":
    sys.exit(main())
