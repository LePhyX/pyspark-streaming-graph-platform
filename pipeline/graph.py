from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import CHECKPOINT_PATH


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


def compute_metrics(graph: GraphFrame) -> dict:
    """Compute degree centrality and connected components; return as Pandas DataFrames."""
    graph.vertices.sparkSession.sparkContext.setCheckpointDir(CHECKPOINT_PATH)
    return {
        "degrees":    graph.degrees.toPandas(),
        "components": graph.connectedComponents().toPandas(),
        "vertices":   graph.vertices.toPandas(),
        "edges":      graph.edges.toPandas(),
    }
