import os
from pymongo import MongoClient

# En production, on utilise des variables d'environnement. 
# Si la Personne 3 utilise MongoDB Atlas, l'URI ressemblera à ça :
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://<username>:<password>@cluster.mongodb.net/myFirstDatabase")

client = MongoClient(MONGO_URI)
# On récupère la base de données
db = client["recommendation_db"]

def get_db():
    return db