import os
import sys
import zipfile
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# ---- Config ----
BUCKET      = os.getenv("S3_BUCKET", "ecommerce-recommendation-2026-225934517672-eu-north-1-an")
INPUT_CSV   = "/tmp/dataset-cleaned.csv"
MODEL_DIR   = "/tmp/als_model_new"
MODEL_ZIP   = "/tmp/als_model.zip"

print("=== Entraînement du modèle ALS ===")

# ---- Démarrer Spark ----
spark = SparkSession.builder \
    .appName("ALS_Retrain") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark démarré")

# ---- Lire le dataset nettoyé ----
print(f"Lecture de {INPUT_CSV}...")
df = spark.read.csv(INPUT_CSV, header=True, inferSchema=True)
print(f"   {df.count():,} lignes")

# ---- Encodage des IDs String → Int ----
print("Encodage des IDs...")

user_indexer = StringIndexer(
    inputCol="user_id",
    outputCol="user_id_int",
    handleInvalid="keep"
)
df = user_indexer.fit(df).transform(df)

product_indexer = StringIndexer(
    inputCol="product_id",
    outputCol="product_id_int",
    handleInvalid="keep"
)
df = product_indexer.fit(df).transform(df)

df = df.withColumn("user_id_int",    df["user_id_int"].cast(IntegerType()))
df = df.withColumn("product_id_int", df["product_id_int"].cast(IntegerType()))

print("✅ Encodage terminé")

# ---- Split train/test ----
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"   Train : {train_df.count():,} lignes")
print(f"   Test  : {test_df.count():,} lignes")

# ---- Entraînement ALS ----
print("Entraînement ALS...")
als = ALS(
    userCol="user_id_int",
    itemCol="product_id_int",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop"
)

model = als.fit(train_df)
print("✅ Modèle entraîné")

# ---- Évaluation ----
predictions = model.transform(test_df).dropna(subset=["prediction"])

if predictions.count() > 0:
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"   RMSE : {rmse:.4f}")
else:
    print("⚠️ Pas assez de données pour évaluer (coldStart drop) — RMSE ignoré")

# ---- Sauvegarder le modèle ----
print("Sauvegarde du modèle...")
model.write().overwrite().save(MODEL_DIR)
print(f"✅ Modèle sauvegardé : {MODEL_DIR}")

# ---- Compresser en ZIP ----
print("Compression...")
with zipfile.ZipFile(MODEL_ZIP, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(MODEL_DIR):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, '/tmp/')
            zipf.write(file_path, arcname)

print(f"✅ ZIP créé : {MODEL_ZIP}")

# ---- Upload sur S3 ----
print("Upload du modèle sur S3...")
s3 = boto3.client('s3')
s3.upload_file(
    MODEL_ZIP,
    BUCKET,
    'final_cleaned/Entrainement_ALS.zip'
)
print("✅ Modèle uploadé sur S3")

# ---- Copier localement pour l'API ----
import shutil

new_model_local = "/home/ubuntu/model/als_model"
tmp_backup      = "/home/ubuntu/model/als_model_backup"

# Backup de l'ancien modèle
if os.path.exists(new_model_local):
    if os.path.exists(tmp_backup):
        shutil.rmtree(tmp_backup)
    shutil.copytree(new_model_local, tmp_backup)
    shutil.rmtree(new_model_local)

# Copier le nouveau modèle
os.makedirs(new_model_local, exist_ok=True)
shutil.copytree(MODEL_DIR, os.path.join(new_model_local, "als_model"))
print("✅ Modèle local mis à jour : /home/ubuntu/model/als_model/als_model")

spark.stop()
print("=== Entraînement terminé ===")