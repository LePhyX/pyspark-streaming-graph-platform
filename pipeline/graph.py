from graphframes import GraphFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.settings import CHECKPOINT_PATH


def build_vertices(batch_df: DataFrame) -> DataFrame:
    """Extract deduplicated User, Seller, Product nodes from a micro-batch."""
    users = (
        batch_df
        .filter(F.col("user_id").isNotNull())
        .select(F.col("user_id").alias("id"))
        .withColumn("type", F.lit("User"))
        .withColumn("label", F.col("id"))
    )
    products = (
        batch_df
        .filter(F.col("product_id").isNotNull())
        .select(F.col("product_id").alias("id"))
        .withColumn("type", F.lit("Product"))
        .withColumn("label", F.col("id"))
    )
    sellers = (
        batch_df
        .filter(F.col("seller_id").isNotNull())
        .select(F.col("seller_id").alias("id"))
        .withColumn("type", F.lit("Seller"))
        .withColumn("label", F.col("id"))
    )
    return users.union(products).union(sellers).dropDuplicates(["id"])


def build_edges(batch_df: DataFrame) -> DataFrame:
    """Build typed directed edges (U→P and S→P) from a micro-batch."""
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
    vertices = build_vertices(batch_df)
    edges = build_edges(batch_df)
    return GraphFrame(vertices, edges)


def compute_metrics(graph: GraphFrame) -> dict:
    """Compute degree centrality and connected components; return as Pandas DataFrames."""
    graph.vertices.sparkSession.sparkContext.setCheckpointDir(CHECKPOINT_PATH)
    degrees_pd = graph.degrees.toPandas()
    components_pd = graph.connectedComponents().toPandas()
    return {
        "degrees": degrees_pd,
        "components": components_pd,
        "vertices": graph.vertices.toPandas(),
        "edges": graph.edges.toPandas(),
    }
