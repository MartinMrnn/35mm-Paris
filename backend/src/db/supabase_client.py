from functools import lru_cache
from supabase import Client, create_client

from config.settings import get_settings


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """Get or create Supabase client (singleton pattern)."""
    settings = get_settings()
    return create_client(settings.supabase_url, settings.supabase_key)


# Pour la compatibilité avec le code existant
# En production, cela créera le client à la première utilisation
# En test, cela sera mocké
class LazySupabaseClient:
    """Lazy loading wrapper for Supabase client."""
    
    def __getattr__(self, name):
        # Déléguer tous les appels au vrai client
        return getattr(get_supabase_client(), name)


# Instance unique utilisée partout
supabase = LazySupabaseClient()