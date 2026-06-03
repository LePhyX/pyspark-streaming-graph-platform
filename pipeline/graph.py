import os

from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import CHECKPOINT_PATH, GRAPH_EDGES_PATH, GRAPH_VERTICES_PATH


def build_vertices(batch_df: DataFrame) -> DataFrame:
    """Extract deduplicated User, Seller, Product nodes from a micro-batch."""
    users = (
        batch_df
        .filter(F.col("user_id").isNotNull())
        .select(
            F.col("user_id").alias("id"),
            F.lit("User").alias("type"),
            F.col("user_city").alias("label"),
        )
    )
    products = (
        batch_df
        .filter(F.col("product_id").isNotNull())
        .select(
            F.col("product_id").alias("id"),
            F.lit("Product").alias("type"),
            F.col("product_cat").alias("label"),
        )
    )
    sellers = (
        batch_df
        .filter(F.col("seller_id").isNotNull())
        .select(
            F.col("seller_id").alias("id"),
            F.lit("Seller").alias("type"),
            F.col("seller_id").alias("label"),
        )
    )
    return users.union(products).union(sellers).dropDuplicates(["id"])


def build_edges(batch_df: DataFrame) -> DataFrame:
    """Build typed directed edges (U→P: AIME/VOUT/ACHAT, S→P: PROPOSE) from a micro-batch."""
    user_product = (
        batch_df
        .filter(
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("action_type").isin("AIME", "VOUT", "ACHAT")
        )
        .select(
            F.col("user_id").alias("src"),
            F.col("product_id").alias("dst"),
            F.col("action_type").alias("relationship"),
        )
    )
    seller_product = (
        batch_df
        .filter(
            F.col("seller_id").isNotNull()
            & F.col("product_id").isNotNull()
        )
        .select(
            F.col("seller_id").alias("src"),
            F.col("product_id").alias("dst"),
            F.lit("PROPOSE").alias("relationship"),
        )
        .dropDuplicates(["src", "dst"])
    )
    return user_product.union(seller_product)


def build_graph(batch_df: DataFrame) -> GraphFrame:
    """Instantiate a GraphFrame from a micro-batch DataFrame."""
    return GraphFrame(build_vertices(batch_df), build_edges(batch_df))


def _df_to_pandas(df):
    """Convert a Spark DataFrame to Pandas via collect(), bypassing Arrow/toPandas issues."""
    import pandas as pd
    return pd.DataFrame([row.asDict() for row in df.collect()])


def _write_csv(df, path: str) -> None:
    """Write a Spark DataFrame to a single CSV file without going through pandas."""
    import csv
    rows = df.collect()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=df.columns)
        writer.writeheader()
        writer.writerows([row.asDict() for row in rows])


def compute_metrics(graph: GraphFrame, export: bool = True) -> dict:
    """Compute degree centrality and connected components; return as Pandas DataFrames.

    If export=True, writes vertices and edges to CSV so the dashboard can consume them.
    Uses collect() instead of toPandas() for Python 3.12 / Arrow compatibility.
    """
    graph.vertices.sparkSession.sparkContext.setCheckpointDir(CHECKPOINT_PATH)

    vertices_pd   = _df_to_pandas(graph.vertices)
    edges_pd      = _df_to_pandas(graph.edges)
    degrees_pd    = _df_to_pandas(graph.degrees)
    components_pd = _df_to_pandas(graph.connectedComponents())

    if export:
        _write_csv(graph.vertices, GRAPH_VERTICES_PATH)
        _write_csv(graph.edges,    GRAPH_EDGES_PATH)

    return {
        "degrees":    degrees_pd,
        "components": components_pd,
        "vertices":   vertices_pd,
        "edges":      edges_pd,
    }
