from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

def download_from_s3():
    s3 = boto3.client('s3')
    bucket = "ecommerce-recommendation-2026-225934517672-eu-north-1-an"
    s3.download_file(bucket, 'data/dataset-ALS.csv', '/tmp/dataset-ALS.csv')
    print("✅ Dataset téléchargé")

with DAG(
    dag_id='retrain_pipeline',
    description='S3 → nettoyage → réentraînement ALS → redéploiement API',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['recommendation', 'als', 'pfa']
) as dag:

    telecharger_data = PythonOperator(
        task_id='telecharger_data',
        python_callable=download_from_s3
    )

    nettoyer = BashOperator(
        task_id='nettoyer_donnees',
        bash_command='''
            cd /opt/airflow/project &&
            pip install pandas boto3 -q &&
            python3 data/clean_data.py &&
            echo "Nettoyage terminé"
        '''
    )

    entrainer = BashOperator(
        task_id='entrainer_modele',
        bash_command='''
            export SPARK_HOME=/opt/spark &&
            export PATH=$PATH:/opt/spark/bin &&
            export PYSPARK_PYTHON=python3 &&
            export S3_BUCKET=ecommerce-recommendation-2026-225934517672-eu-north-1-an &&
            cd /opt/airflow/project &&
            pip install pyspark boto3 numpy pandas -q &&
            spark-submit spark/train_model.py &&
            echo "Modèle réentraîné"
        '''
    )

   redeployer = BashOperator(
       task_id='redeployer_api',
       bash_command='''
           DOCKER_API_VERSION=1.44 docker restart recommendation-api &&
           echo "API redéployée"
       '''
   )

    telecharger_data >> nettoyer >> entrainer >> redeployer