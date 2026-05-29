import os
from pymongo import MongoClient
from pymongo.errors import ConfigurationError

# On garde la variable globale vide au démarrage
_db_client = None

def get_db():
    global _db_client
    
    # Si le client n'a pas encore été initialisé, on le crée
    if _db_client is None:
        try:
            uri = os.getenv("MONGO_URI")
            
            # Options magiques : on augmente les timeouts et on désactive le retry SRV trop lourd
            _db_client = MongoClient(
                uri,
                connectTimeoutMS=5000,      # Attend max 5 secondes pour se connecter
                serverSelectionTimeoutMS=5000, # Attend max 5 secondes pour choisir le serveur
                retryWrites=False
            )
        except Exception as e:
            print(f"❌ Impossible d'initialiser MongoClient au démarrage : {e}")
            
    # On retourne la base de données (si _db_client est initialisé)
    # Remplace "ecommerce-db" par le nom exact si nécessaire
    return _db_client["ecommerce-db"] if _db_client else None