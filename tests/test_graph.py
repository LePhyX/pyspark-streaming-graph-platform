import pytest
from pipeline.spark_session import get_spark_session
from pipeline.schema import EVENT_SCHEMA
from pipeline.graph import build_vertices, build_edges, build_graph


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("TestGraph")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def sample_batch(spark):
    data = [
        ("u1", "p1", "s1", "AIME",    "2024-01-01T00:00:00Z"),
        ("u2", "p1", "s1", "ACHAT",   "2024-01-01T00:00:01Z"),
        ("u1", "p2", "s2", "VOUT",    "2024-01-01T00:00:02Z"),
        (None, "p1", "s1", "PROPOSE", "2024-01-01T00:00:03Z"),
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


def test_build_edges_relationships(sample_batch):
    edges = build_edges(sample_batch)
    relationships = {r["relationship"] for r in edges.collect()}
    assert relationships <= {"AIME", "VOUT", "ACHAT", "PROPOSE"}


def test_build_edges_user_product_direction(sample_batch):
    edges = build_edges(sample_batch)
    user_edges = [r for r in edges.collect() if r["relationship"] in ("AIME", "VOUT", "ACHAT")]
    for e in user_edges:
        assert e["src"].startswith("u")
        assert e["dst"].startswith("p")


def test_build_edges_seller_product_direction(sample_batch):
    edges = build_edges(sample_batch)
    seller_edges = [r for r in edges.collect() if r["relationship"] == "PROPOSE"]
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
