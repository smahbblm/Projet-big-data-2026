from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
import os

def load_model():
    final_path = "/home/ubuntu/model/als_model/als_model"

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