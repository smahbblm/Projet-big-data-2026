import pandas as pd
import os
import boto3

BUCKET = os.getenv("S3_BUCKET", "ecommerce-recommendation-2026-225934517672-eu-north-1-an")
INPUT  = "/tmp/dataset-ALS.csv"
OUTPUT = "/tmp/dataset-cleaned.csv"

print("=== Nettoyage des données ===")

df = pd.read_csv(INPUT)
print(f"   {df.shape[0]:,} lignes, {df.shape[1]} colonnes")
print(f"   Colonnes : {df.columns.tolist()}")

# Renommer seulement si les colonnes originales existent
if 'reviewerID' in df.columns:
    df = df.rename(columns={
        'reviewerID': 'user_id',
        'asin':       'product_id',
        'overall':    'rating'
    })

# Garder seulement les colonnes utiles
df = df[['user_id', 'product_id', 'rating']]

# Nettoyage
avant = len(df)
df = df.dropna()
df = df.drop_duplicates()
print(f"   Lignes supprimées : {avant - len(df):,}")

# Types
df['user_id']    = df['user_id'].astype(str)
df['product_id'] = df['product_id'].astype(str)
df['rating']     = df['rating'].astype(float)

# Filtrer ratings valides
df = df[df['rating'].between(1.0, 5.0)]
print(f"   Lignes finales : {len(df):,}")

# Sauvegarder
df.to_csv(OUTPUT, index=False)
print(f"✅ Dataset nettoyé : {OUTPUT}")

# Upload S3
print("Upload sur S3...")
s3 = boto3.client('s3')
s3.upload_file(OUTPUT, BUCKET, 'data/dataset-cleaned.csv')
print("✅ Upload terminé")