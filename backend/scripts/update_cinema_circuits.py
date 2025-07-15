#!/usr/bin/env python3
"""
Script pour enrichir les cinémas avec leur circuit (chaîne).
Usage: python update_cinema_circuits.py
"""
import sys
import time
from pathlib import Path
from typing import Dict, Set

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.supabase_client import supabase
from db.insert_logic import cinema_id_to_int
from allocineAPI.allocineAPI import allocineAPI
from utils.logger import get_logger

logger = get_logger(__name__)

DELAY_BETWEEN_REQUESTS = 0.5  # Pour ne pas surcharger l'API


def update_cinema_circuits():
    """Met à jour les circuits pour tous les cinémas."""
    api = allocineAPI()
    
    logger.info("Récupération des circuits disponibles...")
    
    try:
        # 1. Récupérer tous les circuits
        circuits = api.get_circuit()
        logger.info(f"Trouvé {len(circuits)} circuits")
        
        # 2. Pour chaque circuit, récupérer ses cinémas
        cinema_to_circuit: Dict[str, Dict[str, str]] = {}
        
        for circuit in circuits:
            circuit_id = circuit['id']
            circuit_name = circuit['name']
            
            logger.info(f"Traitement du circuit: {circuit_name} ({circuit_id})")
            
            try:
                # Récupérer les cinémas de ce circuit
                cinemas_in_circuit = api.get_cinema(circuit_id)
                
                logger.info(f"  → {len(cinemas_in_circuit)} cinémas dans ce circuit")
                
                # Mapper chaque cinéma à son circuit
                for cinema in cinemas_in_circuit:
                    cinema_id = cinema['id']
                    cinema_to_circuit[cinema_id] = {
                        'circuit_id': circuit_id,
                        'circuit_name': circuit_name
                    }
                
                # Pause entre les requêtes
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
            except Exception as e:
                logger.error(f"Erreur pour le circuit {circuit_name}: {e}")
                continue
        
        # 3. Mettre à jour la base de données
        logger.info(f"Mise à jour de {len(cinema_to_circuit)} cinémas avec leur circuit...")
        
        updated_count = 0
        errors_count = 0
        
        for cinema_id_str, circuit_info in cinema_to_circuit.items():
            try:
                # Convertir l'ID pour la base
                cinema_id_int = cinema_id_to_int(cinema_id_str)
                
                # Mettre à jour le cinéma
                result = supabase.table("cinemas").update({
                    "circuit_id": circuit_info['circuit_id']
                }).eq("id", cinema_id_int).execute()
                
                if result.data:
                    updated_count += 1
                    logger.debug(f"Mis à jour: Cinéma {cinema_id_str} → {circuit_info['circuit_name']}")
                
            except Exception as e:
                errors_count += 1
                logger.error(f"Erreur update cinéma {cinema_id_str}: {e}")
        
        # 4. Rapport final
        logger.info("=== RAPPORT FINAL ===")
        logger.info(f"Circuits trouvés: {len(circuits)}")
        logger.info(f"Cinémas avec circuit identifié: {len(cinema_to_circuit)}")
        logger.info(f"Cinémas mis à jour: {updated_count}")
        logger.info(f"Erreurs: {errors_count}")
        
        # 5. Vérifier les cinémas sans circuit
        cinemas_without_circuit = supabase.table("cinemas").select("id, name").is_("circuit_name", "null").execute()
        
        if cinemas_without_circuit.data:
            logger.info(f"\nCinémas indépendants (sans circuit): {len(cinemas_without_circuit.data)}")
            for cinema in cinemas_without_circuit.data[:10]:
                logger.info(f"  - {cinema['name']}")
            if len(cinemas_without_circuit.data) > 10:
                logger.info(f"  ... et {len(cinemas_without_circuit.data) - 10} autres")
        
        # 6. Afficher quelques exemples de circuits
        logger.info("\nExemples de circuits trouvés:")
        circuit_stats = {}
        for circuit_info in cinema_to_circuit.values():
            name = circuit_info['circuit_name']
            circuit_stats[name] = circuit_stats.get(name, 0) + 1
        
        for circuit_name, count in sorted(circuit_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"  - {circuit_name}: {count} cinémas")
        
    except Exception as e:
        logger.error(f"Erreur générale: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(update_cinema_circuits())