# Initialisation de la SparkSession avec les dépendances GraphFrames.

from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, GRAPHFRAMES_JAR


def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", GRAPHFRAMES_JAR)
        .config("spark.executor.memory", "2g")
        # Réduit le nombre de partitions shuffle (défaut 200, trop élevé en local).
        .config("spark.sql.shuffle.partitions", "4")
        # Le schéma est défini manuellement dans schema.py, on désactive l'inférence.
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
