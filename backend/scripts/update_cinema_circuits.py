#!/usr/bin/env python3
"""
Script pour créer les circuits et les associer aux cinémas.
Usage: python update_cinema_circuits.py
"""
import sys
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from db.supabase_client import supabase
from db.insert_logic import cinema_id_to_int
from allocineAPI.allocineAPI import allocineAPI
from utils.logger import get_logger

logger = get_logger(__name__)

DELAY_BETWEEN_REQUESTS = 0.5


def generate_circuit_id(circuit_code: str) -> int:
    """Génère un ID stable pour un circuit, comme pour les directors."""
    normalized = circuit_code.strip().lower()
    hash_bytes = hashlib.sha256(normalized.encode()).digest()
    return int.from_bytes(hash_bytes[:4], 'big') % 100_000_000


def create_circuits_and_update_cinemas():
    """Crée les circuits et met à jour les cinémas."""
    api = allocineAPI()
    
    logger.info("=== ÉTAPE 1: Récupération des circuits ===")
    
    try:
        # 1. Récupérer tous les circuits
        circuits_data = api.get_circuit()
        logger.info(f"Trouvé {len(circuits_data)} circuits")
        
        # 2. Insérer les circuits dans la base
        circuits_to_insert = []
        circuit_mapping = {}  # circuit_code -> circuit_id
        
        for circuit in circuits_data:
            circuit_code = circuit['id']
            circuit_name = circuit['name']
            circuit_id = generate_circuit_id(circuit_code)
            
            circuits_to_insert.append({
                'id': circuit_id,
                'code': circuit_code,
                'name': circuit_name
            })
            
            circuit_mapping[circuit_code] = circuit_id
            logger.info(f"Circuit: {circuit_name} → ID: {circuit_id}")
        
        # Bulk insert des circuits
        if circuits_to_insert:
            result = supabase.table("circuits").upsert(
                circuits_to_insert,
                on_conflict="id"
            ).execute()
            logger.info(f"✅ {len(circuits_to_insert)} circuits insérés/mis à jour")
        
        # 3. Pour chaque circuit, récupérer ses cinémas et les mettre à jour
        logger.info("\n=== ÉTAPE 2: Association cinémas-circuits ===")
        
        total_updates = 0
        cinema_updates = []  # Pour bulk update
        
        for circuit in circuits_data:
            circuit_code = circuit['id']
            circuit_name = circuit['name']
            circuit_id = circuit_mapping[circuit_code]
            
            logger.info(f"\nTraitement: {circuit_name}")
            
            try:
                # Récupérer les cinémas de ce circuit
                cinemas_in_circuit = api.get_cinema(circuit_code)
                logger.info(f"  → {len(cinemas_in_circuit)} cinémas trouvés")
                
                # Préparer les updates
                for cinema in cinemas_in_circuit:
                    cinema_id_str = cinema['id']
                    cinema_id_int = cinema_id_to_int(cinema_id_str)
                    
                    cinema_updates.append({
                        'id': cinema_id_int,
                        'circuit_id': circuit_id
                    })
                
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
            except Exception as e:
                logger.error(f"Erreur pour le circuit {circuit_name}: {e}")
                continue
        
        # 4. Bulk update des cinémas
        if cinema_updates:
            # Faire les updates par batch de 100
            batch_size = 100
            for i in range(0, len(cinema_updates), batch_size):
                batch = cinema_updates[i:i + batch_size]
                
                for update in batch:
                    try:
                        result = supabase.table("cinemas").update({
                            'circuit_id': update['circuit_id']
                        }).eq('id', update['id']).execute()
                        
                        if result.data:
                            total_updates += 1
                    except Exception as e:
                        logger.error(f"Erreur update cinéma {update['id']}: {e}")
            
            logger.info(f"\n✅ {total_updates} cinémas mis à jour avec leur circuit")
        
        # 5. Statistiques finales
        logger.info("\n=== STATISTIQUES ===")
        
        # Cinémas par circuit
        stats_query = """
        SELECT 
            ci.name as circuit_name, 
            COUNT(c.id) as nb_cinemas
        FROM circuits ci
        LEFT JOIN cinemas c ON c.circuit_id = ci.id
        GROUP BY ci.name
        ORDER BY nb_cinemas DESC
        """
        
        # Utiliser une requête plus simple compatible avec Supabase
        circuits_with_counts = []
        for circuit in circuits_to_insert:
            count_result = supabase.table("cinemas").select("id", count="exact").eq("circuit_id", circuit['id']).execute()
            if count_result.count > 0:
                circuits_with_counts.append({
                    'name': circuit['name'],
                    'count': count_result.count
                })
        
        circuits_with_counts.sort(key=lambda x: x['count'], reverse=True)
        
        logger.info("\nCinémas par circuit:")
        for item in circuits_with_counts:
            logger.info(f"  - {item['name']}: {item['count']} cinémas")
        
        # Cinémas indépendants
        independents = supabase.table("cinemas").select("id, name").is_("circuit_id", "null").execute()
        
        if independents.data:
            logger.info(f"\nCinémas indépendants: {len(independents.data)}")
            for cinema in independents.data[:5]:
                logger.info(f"  - {cinema['name']}")
            if len(independents.data) > 5:
                logger.info(f"  ... et {len(independents.data) - 5} autres")
        
        return 0
        
    except Exception as e:
        logger.error(f"Erreur générale: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(create_circuits_and_update_cinemas())