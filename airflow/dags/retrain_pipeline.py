from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='retrain_pipeline',
    description='S3 → nettoyage → réentraînement ALS → redéploiement API',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['recommendation', 'als', 'pfa']
) as dag:

    telecharger_data = BashOperator(
        task_id='telecharger_data',
        bash_command='''
            aws s3 cp s3://ecommerce-recommendation-2026-225934517672-eu-north-1-an/data/dataset-ALS.csv \
            /tmp/dataset-ALS.csv && echo "Dataset téléchargé"
        '''
    )

    nettoyer = BashOperator(
        task_id='nettoyer_donnees',
        bash_command='''
            cd /opt/airflow/project &&
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
            cd /opt/airflow/project &&
            spark-submit spark/train_model.py &&
            echo "Modèle réentraîné"
        '''
    )

    upload_modele = BashOperator(
        task_id='upload_modele_s3',
        bash_command='''
            aws s3 cp /tmp/als_model.zip \
            s3://ecommerce-recommendation-2026-225934517672-eu-north-1-an/final_cleaned/Entrainement_ALS.zip &&
            echo "Modèle uploadé sur S3"
        '''
    )

    redeployer = BashOperator(
        task_id='redeployer_api',
        bash_command='''
            docker restart recommendation-api &&
            echo "API redéployée"
        '''
    )

    telecharger_data >> nettoyer >> entrainer >> upload_modele >> redeployer