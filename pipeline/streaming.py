from pipeline.spark_session import get_spark_session
from pipeline.schema import EVENT_SCHEMA
from pipeline.graph import build_graph, compute_metrics
from config.settings import DATA_PATH, CHECKPOINT_PATH

# Shared in-memory state updated by foreachBatch — consumed by the dashboard
_graph_state: dict = {
    "degrees": None,
    "components": None,
    "vertices": None,
    "edges": None,
}


def get_graph_state() -> dict:
    return _graph_state


def _process_batch(batch_df, epoch_id: int) -> None:
    if batch_df.count() == 0:
        return
    graph = build_graph(batch_df)
    metrics = compute_metrics(graph)
    _graph_state.update(metrics)


def start_streaming():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    stream_df = (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .json(DATA_PATH)
    )

    query = (
        stream_df
        .writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH + "streaming/")
        .start()
    )
    return query
