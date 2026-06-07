from fastapi import FastAPI
from model_loader import load_model
from database import get_db
from dotenv import load_dotenv
from datetime import datetime
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

load_dotenv()

app = FastAPI()

print("Chargement du modèle...")
spark, model = load_model()
db = get_db()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/recommend/{user_id}")
def recommend(user_id: int, n: int = 5):
    user_id_clean = int(user_id)

    try:
        df = spark.createDataFrame([(user_id_clean,)], ["user_id_int"])
        recs = model.recommendForUserSubset(df, n)
        recommendations = []

        if not recs.isEmpty():
            collected = recs.collect()
            if len(collected) > 0:
                for row in collected[0].recommendations:
                    recommendations.append({
                        "product_id": int(row[0]),
                        "score": float(row[1])
                    })

        if recommendations:
            result = {
                "user_id": user_id_clean,
                "recommendations": recommendations,
                "timestamp": datetime.now().isoformat()
            }
            try:
                if db is not None:
                    db["Recommendations"].insert_one(result)
                    result.pop("_id", None)
            except Exception as e:
                print(f"⚠️ MongoDB write error: {e}")
                result.pop("_id", None)
            return result

    except Exception as e:
        print(f"⚠️ Spark error: {e}")

    try:
        if db is not None:
            cached = db["Recommendations"].find_one(
                {"user_id": user_id_clean}, {"_id": 0}
            )
            if cached:
                return cached
    except Exception as e:
        print(f"⚠️ MongoDB read error: {e}")

    return {
        "user_id": user_id_clean,
        "recommendations": [],
        "message": "Utilisateur non trouvé dans le modèle ALS."
    }