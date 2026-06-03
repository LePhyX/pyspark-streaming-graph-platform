import pytest

from pipeline.graph import build_edges, build_graph, build_vertices
from pipeline.schema import EVENT_SCHEMA
from pipeline.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("TestGraph")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def sample_batch(spark):
    data = [
        ("2024-01-01T00:00:00", "u1", "Paris",  "p1", "Électronique", "s1", "AIME",  199.99),
        ("2024-01-01T00:00:01", "u2", "Lyon",   "p1", "Électronique", "s1", "ACHAT", 199.99),
        ("2024-01-01T00:00:02", "u1", "Paris",  "p2", "Mode",         "s2", "VOUT",  49.90),
        ("2024-01-01T00:00:03", "u3", "Nantes", "p2", "Mode",         "s2", "AIME",  49.90),
    ]
    return spark.createDataFrame(data, EVENT_SCHEMA)


def test_build_vertices_no_duplicates(sample_batch):
    vertices = build_vertices(sample_batch)
    ids = [r["id"] for r in vertices.collect()]
    assert len(ids) == len(set(ids))


def test_build_vertices_contains_all_types(sample_batch):
    vertices = build_vertices(sample_batch)
    types = {r["type"] for r in vertices.collect()}
    assert types == {"User", "Product", "Seller"}


def test_build_edges_user_product_direction(sample_batch):
    edges = build_edges(sample_batch)
    user_edges = [r for r in edges.collect() if r["relationship"] in ("AIME", "VOUT", "ACHAT")]
    assert len(user_edges) > 0
    for e in user_edges:
        assert e["src"].startswith("u")
        assert e["dst"].startswith("p")


def test_build_edges_seller_propose(sample_batch):
    edges = build_edges(sample_batch)
    seller_edges = [r for r in edges.collect() if r["relationship"] == "PROPOSE"]
    assert len(seller_edges) > 0
    for e in seller_edges:
        assert e["src"].startswith("s")
        assert e["dst"].startswith("p")


def test_graph_frame_instantiation(sample_batch):
    graph = build_graph(sample_batch)
    assert graph.vertices.count() > 0
    assert graph.edges.count() > 0


def test_graph_degrees_non_null(sample_batch):
    graph = build_graph(sample_batch)
    degrees = graph.degrees.collect()
    assert len(degrees) > 0
    for row in degrees:
        assert row["degree"] >= 1
