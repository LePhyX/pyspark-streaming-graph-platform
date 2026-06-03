from pyspark.sql.types import StructType, StructField, StringType

EVENT_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action", StringType(), False),
    StructField("timestamp", StringType(), True),
])
