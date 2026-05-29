from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
import zipfile
import os

def load_model(zip_path="Entrainement_ALS/als_model/als_model"):
    # 1. Si on nous passe un chemin qui finit par .zip, on gère l'extraction
    if zip_path.endswith(".zip"):
        extract_dir = zip_path.replace(".zip", "")
        if not os.path.exists(extract_dir):
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_dir)
        final_path = extract_dir
    else:
        # Si c'est directement le dossier, on l'utilise tel quel
        final_path = zip_path

    # 2. Initialisation de Spark avec les patchs pour Windows
    spark = SparkSession.builder \
        .appName("RecoAPI") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.warehouse.dir", "file:///D:/tmp") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()

    # 3. Désactiver la vérification NativeIO de Hadoop en Java pour Windows
    try:
        spark._jvm.org.apache.hadoop.io.nativeio.NativeIO.Windows.setAllowAllAccess(True)
    except Exception as e:
        print("Note: Impossible de patcher NativeIO (Hadoop est peut-être absent), on continue...")

    # 4. Chargement du modèle depuis le BON chemin final
    model = ALSModel.load(final_path)
    
    return spark, model