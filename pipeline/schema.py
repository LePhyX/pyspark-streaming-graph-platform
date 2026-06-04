# Schéma Spark des événements JSON produits par le simulateur.
# Déclaré explicitement pour éviter l'inférence automatique et garantir le typage dès la lecture.

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType([
    StructField("timestamp",   TimestampType(), True),
    StructField("user_id",     StringType(),    True),
    StructField("user_city",   StringType(),    True),
    StructField("product_id",  StringType(),    True),
    StructField("product_cat", StringType(),    True),
    StructField("seller_id",   StringType(),    True),
    StructField("action_type", StringType(),    True),
    StructField("price",       DoubleType(),    True),
])
