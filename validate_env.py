"""
Validation script: checks that SparkSession + GraphFrames work correctly in local mode.
Run with: python validate_env.py
"""
import sys
from pipeline.spark_session import get_spark_session
from graphframes import GraphFrame


def main():
    print("── Démarrage SparkSession...")
    spark = get_spark_session("ValidateEnv")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"   Spark version : {spark.version}  ✓")

    print("── Construction des vertices...")
    vertices = spark.createDataFrame([
        ("u1", "User",    "Alice"),
        ("p1", "Product", "Vélo VTT"),
        ("s1", "Seller",  "Bob"),
    ], ["id", "type", "label"])
    vertices.show()

    print("── Construction des edges...")
    edges = spark.createDataFrame([
        ("u1", "p1", "AIME"),
        ("s1", "p1", "PROPOSE"),
    ], ["src", "dst", "relationship"])
    edges.show()

    print("── Instanciation du GraphFrame...")
    g = GraphFrame(vertices, edges)

    print("── graph.degrees :")
    g.degrees.show()

    print("── Validation réussie ✓")
    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
