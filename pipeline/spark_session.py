from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, GRAPHFRAMES_JAR


def get_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", GRAPHFRAMES_JAR)
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
