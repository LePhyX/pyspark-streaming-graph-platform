from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, window

from config.settings import (
    CHECKPOINT_PATH,
    DATA_PATH,
    SLIDE_DURATION,
    WATERMARK_DELAY,
    WINDOW_DURATION,
)
from pipeline.graph import build_graph, compute_metrics
from pipeline.schema import EVENT_SCHEMA


def read_stream(spark: SparkSession, path: str = DATA_PATH) -> DataFrame:
    return (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(path)
    )


def build_action_window_agg(stream_df: DataFrame) -> DataFrame:
    """Sliding window (1 min / 30s): event count per action type.
    Output mode: update — emits only changed rows each trigger."""
    return (
        stream_df
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("action_type"),
        )
        .agg(count("*").alias("event_count"))
    )


def build_purchase_by_category(stream_df: DataFrame) -> DataFrame:
    """Tumbling window (1 min): purchase count per product category.
    Output mode: update — watermark bounds the state, update avoids full recompute."""
    return (
        stream_df
        .filter(col("action_type") == "ACHAT")
        .withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION),
            col("product_cat"),
        )
        .agg(count("*").alias("purchase_count"))
    )


def _write_graph_batch(batch_df: DataFrame, epoch_id: int) -> None:
    if batch_df.count() == 0:
        return
    graph = build_graph(batch_df)
    compute_metrics(graph, export=True)


def start_queries(spark: SparkSession, stream_df: DataFrame):
    action_agg = build_action_window_agg(stream_df)
    purchase_agg = build_purchase_by_category(stream_df)

    q1 = (
        action_agg.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("action_counts")
        .option("checkpointLocation", CHECKPOINT_PATH + "action_counts")
        .trigger(processingTime="5 seconds")
        .start()
    )

    q2 = (
        purchase_agg.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("purchase_by_category")
        .option("checkpointLocation", CHECKPOINT_PATH + "purchase_by_category")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Graph foreachBatch: builds GraphFrame and exports vertices/edges CSVs each trigger
    q3 = (
        stream_df.writeStream
        .foreachBatch(_write_graph_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "graph")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return q1, q2, q3


if __name__ == "__main__":
    from pipeline.spark_session import get_spark_session

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    stream_df = read_stream(spark)
    q1, q2, q3 = start_queries(spark, stream_df)

    print("Pipeline démarrée. Ctrl+C pour arrêter.")
    spark.streams.awaitAnyTermination()
