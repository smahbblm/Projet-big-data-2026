from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
import zipfile
import os
import boto3

def download_from_s3():
    bucket   = os.getenv("S3_BUCKET")
    s3_key   = os.getenv("S3_MODEL_KEY")
    zip_path = "/tmp/model/Entrainement_ALS.zip"
    model_dir = "/tmp/model/als_model"

    os.makedirs("/tmp/model", exist_ok=True)

    if not os.path.exists(zip_path):
        print("Téléchargement du modèle depuis S3...")
        s3 = boto3.client('s3')
        s3.download_file(bucket, s3_key, zip_path)
        print("✅ ZIP téléchargé")

    if not os.path.exists(model_dir):
        print("Extraction du modèle...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall("/tmp/model/")
        print("✅ Modèle extrait")

    # Trouver le bon chemin
    final_path = "/tmp/model/als_model/als_model"
    if not os.path.exists(final_path):
        final_path = "/tmp/model/als_model"

    return final_path

def load_model():
    final_path = download_from_s3()

    spark = SparkSession.builder \
        .appName("RecoAPI") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark démarré")

    model = ALSModel.load(final_path)
    print(f"✅ Modèle chargé depuis {final_path}")

    return spark, model