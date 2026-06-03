"""
Streamlit dashboard — Streaming Graph Platform
Run: streamlit run dashboard/app.py
"""
import glob
import json
import os
import time

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import streamlit as st

from config.settings import (
    DASHBOARD_REFRESH_MS,
    DATA_PATH,
    GRAPH_EDGES_PATH,
    GRAPH_VERTICES_PATH,
)

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Streaming Graph Dashboard",
    page_icon="🔗",
    layout="wide",
)

REFRESH_S = DASHBOARD_REFRESH_MS / 1000

NODE_COLORS = {
    "User":    "#4C9BE8",
    "Seller":  "#5EBD7A",
    "Product": "#F4A442",
}
DEFAULT_COLOR = "#AAAAAA"


# ─── Data loaders ─────────────────────────────────────────────────────────────

def load_graph_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    if not os.path.exists(GRAPH_VERTICES_PATH) or not os.path.exists(GRAPH_EDGES_PATH):
        return None, None
    return pd.read_csv(GRAPH_VERTICES_PATH), pd.read_csv(GRAPH_EDGES_PATH)


def load_events() -> pd.DataFrame:
    files = glob.glob(os.path.join(DATA_PATH, "*.json"))
    if not files:
        return pd.DataFrame()
    records = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                records.append(json.load(fh))
        except (json.JSONDecodeError, OSError):
            continue
    return pd.DataFrame(records) if records else pd.DataFrame()


# ─── Graph renderer ───────────────────────────────────────────────────────────

def draw_graph(vertices: pd.DataFrame, edges: pd.DataFrame) -> plt.Figure:
    G = nx.DiGraph()
    for _, row in vertices.iterrows():
        G.add_node(row["id"], node_type=row.get("type", ""))
    for _, row in edges.iterrows():
        G.add_edge(row["src"], row["dst"], label=row.get("relationship", ""))

    pos = nx.spring_layout(G, seed=42, k=1.8)
    node_colors = [
        NODE_COLORS.get(G.nodes[n].get("node_type", ""), DEFAULT_COLOR)
        for n in G.nodes
    ]
    edge_labels = nx.get_edge_attributes(G, "label")

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#0E1117")
    ax.set_facecolor("#0E1117")

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=700, ax=ax)
    nx.draw_networkx_labels(G, pos, font_color="white", font_size=7, ax=ax)
    nx.draw_networkx_edges(
        G, pos, edge_color="#CCCCCC", arrows=True, arrowsize=15,
        connectionstyle="arc3,rad=0.1", ax=ax,
    )
    nx.draw_networkx_edge_labels(
        G, pos, edge_labels=edge_labels, font_size=6, font_color="#FFDD88", ax=ax,
    )
    ax.axis("off")
    return fig


# ─── Layout ──────────────────────────────────────────────────────────────────

st.title("🔗 Streaming Graph Platform — Dashboard")

vertices, edges = load_graph_data()
events = load_events()

col_graph, col_metrics = st.columns([2, 1])

with col_graph:
    st.subheader("Graphe en temps réel")
    if vertices is not None and not vertices.empty and edges is not None and not edges.empty:
        st.pyplot(draw_graph(vertices, edges))
        st.caption(f"{len(vertices)} nœuds · {len(edges)} arêtes")

        with st.expander("Top degrees (nœuds les plus connectés)"):
            deg = (
                pd.concat([edges[["src"]], edges[["dst"]].rename(columns={"dst": "src"})])
                .value_counts()
                .reset_index()
            )
            deg.columns = ["nœud", "degree"]
            st.dataframe(deg.head(10), use_container_width=True)
    else:
        st.info("En attente des données du pipeline… Lancez d'abord `pipeline/streaming.py`.")

with col_metrics:
    # Legend
    st.subheader("Légende")
    for node_type, color in NODE_COLORS.items():
        st.markdown(
            f"<span style='color:{color}; font-size:18px'>■</span> **{node_type}**",
            unsafe_allow_html=True,
        )

    st.divider()

    if not events.empty and "action_type" in events.columns:
        st.subheader("Actions par type")
        counts = events["action_type"].value_counts().rename_axis("action").reset_index(name="count")
        st.bar_chart(counts.set_index("action"))

        st.subheader("Achats par catégorie")
        purchases = events[events["action_type"] == "ACHAT"]
        if not purchases.empty and "product_cat" in purchases.columns:
            cat = purchases["product_cat"].value_counts().rename_axis("catégorie").reset_index(name="achats")
            st.dataframe(cat, use_container_width=True)
        else:
            st.caption("Aucun achat dans les données actuelles.")
    else:
        st.info("Aucun événement reçu pour l'instant.")

    if vertices is not None and not vertices.empty:
        st.divider()
        st.subheader("Nœuds par type")
        type_counts = vertices["type"].value_counts().rename_axis("type").reset_index(name="count")
        st.dataframe(type_counts, use_container_width=True)

# ─── Auto-refresh ─────────────────────────────────────────────────────────────

st.caption(f"⟳ Rafraîchissement automatique toutes les {int(REFRESH_S)} secondes")
time.sleep(REFRESH_S)
st.rerun()
