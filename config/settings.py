# Paramètres centralisés de la plateforme (chemins, durées de fenêtres, configuration Spark).

SPARK_APP_NAME = "StreamingGraphPlatform"
GRAPHFRAMES_JAR = "graphframes:graphframes:0.8.3-spark3.5-s_2.12"

DATA_PATH            = "data/events/"
CHECKPOINT_PATH      = "checkpoints/"
GRAPH_VERTICES_PATH  = "data/graph_vertices.csv"
GRAPH_EDGES_PATH     = "data/graph_edges.csv"

WATERMARK_DELAY    = "10 seconds"
WINDOW_DURATION    = "1 minute"
SLIDE_DURATION     = "30 seconds"

DASHBOARD_REFRESH_MS = 5000
