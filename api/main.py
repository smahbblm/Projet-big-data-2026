from fastapi import FastAPI, HTTPException
from model_loader import load_model
from database import get_db
from dotenv import load_dotenv
from datetime import datetime

import os
import sys

# 1. Configuration Hadoop & Windows
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] += r";D:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Charger le .env
load_dotenv()

app = FastAPI()

# Charger le modèle
spark, model = load_model("Entrainement_ALS/als_model/als_model")

# Connexion MongoDB
db = get_db()

@app.get("/recommend/{user_id}")
def recommend(user_id: int, n: int = 5):
    user_id_clean = int(user_id)
    
    try:
        # ÉTAPE 1 : Essayer de générer la recommandation en temps réel avec Spark
        df = spark.createDataFrame([(user_id_clean,)], ["user_id_int"])
        recs = model.recommendForUserSubset(df, n)

        recommendations = []
        
        if not recs.isEmpty():
            collected_recs = recs.collect()
            if len(collected_recs) > 0:
                for row in collected_recs[0].recommendations:
                    recommendations.append({
                        "product_id": int(row[0]),  
                        "score": float(row[1])     
                    })

        # Si Spark a trouvé des données, on prépare le dictionnaire
        if recommendations:
            result = {
                "user_id": user_id_clean,
                "recommendations": recommendations,
                "timestamp": datetime.now().isoformat()
            }

            # Sauvegarder dans MongoDB pour l'historique ou le cache
            try:
                db["Recommendations"].insert_one(result)
                # Supprimer l'_id généré par MongoDB pour éviter le crash TypeError de FastAPI
                if "_id" in result:
                    del result["_id"]
            except Exception as mongo_err:
                print(f"⚠️ Erreur de persistence MongoDB : {mongo_err}")
                # On ne bloque pas, on supprime l'_id s'il est présent
                if "_id" in result:
                    del result["_id"]

            return result

    except Exception as spark_err:
        print(f"⚠️ Spark a échoué ou l'utilisateur n'est pas dans le modèle : {spark_err}")
        # On ne crash pas, on passe à l'étape suivante (recherche dans MongoDB)

    # ÉTAPE 2 : CODE DE SÉCURITÉ (Si Spark est vide ou a échoué)
    # On va chercher si on a d'anciennes recommandations enregistrées dans MongoDB
    try:
        cached_recommendation = db["Recommendations"].find_one({"user_id": user_id_clean}, {"_id": 0})
        
        if cached_recommendation:
            return cached_recommendation
    except Exception as mongo_read_err:
        print(f"⚠️ Impossible de lire dans MongoDB : {mongo_read_err}")

    # ÉTAPE 3 : Si MongoDB ET Spark n'ont rien donné, on gère proprement au lieu de crash 500
    return {
        "user_id": user_id_clean,
        "recommendations": [],
        "message": "Aucune donnée trouvée. L'utilisateur n'existe pas dans le modèle ALS et aucune sauvegarde n'est présente dans MongoDB."
    }