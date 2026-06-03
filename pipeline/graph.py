import csv
import os

import pandas as pd
from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import CHECKPOINT_PATH, GRAPH_EDGES_PATH, GRAPH_VERTICES_PATH


def build_vertices(batch_df: DataFrame) -> DataFrame:
    """Extract deduplicated User, Seller, Product nodes from a micro-batch."""
    users = batch_df.filter(F.col("user_id").isNotNull()).select(
        F.col("user_id").alias("id"),
        F.lit("User").alias("type"),
        F.col("user_city").alias("label"),
    )
    products = batch_df.filter(F.col("product_id").isNotNull()).select(
        F.col("product_id").alias("id"),
        F.lit("Product").alias("type"),
        F.col("product_cat").alias("label"),
    )
    sellers = batch_df.filter(F.col("seller_id").isNotNull()).select(
        F.col("seller_id").alias("id"),
        F.lit("Seller").alias("type"),
        F.col("seller_id").alias("label"),
    )
    return users.union(products).union(sellers).dropDuplicates(["id"])


def build_edges(batch_df: DataFrame) -> DataFrame:
    """Build typed directed edges (U→P: AIME/VOUT/ACHAT, S→P: PROPOSE) from a micro-batch."""
    user_edges = batch_df.filter(
        F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("action_type").isin("AIME", "VOUT", "ACHAT")
    ).select(
        F.col("user_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.col("action_type").alias("relationship"),
    )
    seller_edges = batch_df.filter(
        F.col("seller_id").isNotNull() & F.col("product_id").isNotNull()
    ).select(
        F.col("seller_id").alias("src"),
        F.col("product_id").alias("dst"),
        F.lit("PROPOSE").alias("relationship"),
    ).dropDuplicates(["src", "dst"])
    return user_edges.union(seller_edges)


def build_graph(batch_df: DataFrame) -> GraphFrame:
    return GraphFrame(build_vertices(batch_df), build_edges(batch_df))


def _to_pandas(df: DataFrame) -> pd.DataFrame:
    """Convert Spark DataFrame via collect() — avoids Arrow/toPandas issues on Python 3.12."""
    return pd.DataFrame([row.asDict() for row in df.collect()])


def _write_csv(df: DataFrame, path: str) -> None:
    rows = df.collect()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=df.columns)
        writer.writeheader()
        writer.writerows([row.asDict() for row in rows])


def compute_metrics(graph: GraphFrame, export: bool = True) -> dict:
    """Compute degree centrality and connected components.

    If export=True, writes vertices/edges CSVs consumed by the dashboard.
    """
    graph.vertices.sparkSession.sparkContext.setCheckpointDir(CHECKPOINT_PATH)

    if export:
        _write_csv(graph.vertices, GRAPH_VERTICES_PATH)
        _write_csv(graph.edges,    GRAPH_EDGES_PATH)

    return {
        "degrees":    _to_pandas(graph.degrees),
        "components": _to_pandas(graph.connectedComponents()),
        "vertices":   _to_pandas(graph.vertices),
        "edges":      _to_pandas(graph.edges),
    }
