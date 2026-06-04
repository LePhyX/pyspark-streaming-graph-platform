# PySpark Streaming Graph Platform

Pipeline de streaming en temps réel simulant des interactions utilisateur/vendeur/produit sur une place de marché, traitées avec PySpark Structured Streaming et visualisées sous forme de graphe dynamique via GraphFrames.

---

## Prérequis

- Java 11+
- Python 3.10+

> Le JAR GraphFrames (`graphframes:graphframes:0.8.3-spark3.5-s_2.12`) est téléchargé automatiquement depuis Maven au premier démarrage de Spark.

---

## Stack technique

| Composant | Technologie |
|---|---|
| Moteur de traitement | PySpark 3.5 + Spark Structured Streaming |
| Graphe | GraphFrames 0.8.3 |
| Simulation | Faker |
| Dashboard | Streamlit + PyVis |

---

## Structure du projet

```
pyspark-streaming-graph-platform/
│
├── run.sh                        # Lance les 3 composants en parallèle
├── requirements.txt
│
├── config/
│   └── settings.py               # Paramètres centralisés (chemins, fenêtres, refresh)
│
├── simulator/
│   └── event_producer.py         # Génère un événement JSON toutes les 500ms → data/events/
│
├── pipeline/
│   ├── schema.py                 # Schéma Spark de l'événement (typage strict)
│   ├── spark_session.py          # Initialisation SparkSession + GraphFrames
│   ├── graph.py                  # Construction et export du graphe GraphFrames
│   └── streaming.py              # Lecture du stream, agrégation fenêtrée, foreachBatch
│
├── dashboard/
│   └── app.py                    # Interface Streamlit (rafraîchissement toutes les 5s)
│
└── report/                       # Dépôt du rapport
```

---

## Schéma d'un événement

| Champ | Type | Exemple |
|---|---|---|
| `timestamp` | Timestamp | `2026-06-04T14:32:01` |
| `user_id` | String | `usr_9482` |
| `user_city` | String | `Paris` |
| `product_id` | String | `prod_5501` |
| `product_cat` | String | `Électronique` |
| `seller_id` | String | `sel_214` |
| `action_type` | String | `AIME` / `VOUT` / `ACHAT` |
| `price` | Double | `450.00` |

Les actions suivent une répartition pondérée : 60 % AIME, 30 % VOUT, 10 % ACHAT.

---

## Concepts PySpark couverts

| Concept | Implémentation |
|---|---|
| `SparkSession` & `SparkContext` | `pipeline/spark_session.py` |
| Structured Streaming + Schema Enforcement | `pipeline/streaming.py` + `pipeline/schema.py` |
| Windowing (fenêtre glissante 1 min / 30s) | `build_action_window_agg()` dans `streaming.py` |
| Watermarking (`withWatermark`) | délai de 10s sur le timestamp de l'événement |
| Output Mode `update` | seules les lignes modifiées sont émises à chaque déclenchement |
| GraphFrames (vertices + edges) | `pipeline/graph.py` — nœuds User/Product/Seller, arêtes typées |

---

## Flux de données

```
event_producer  →  data/events/*.json  →  streaming.py (Spark)
                                                │
                               ┌────────────────┴──────────────────┐
                               q1 (windowing / watermark)          q2 (foreachBatch)
                               → table mémoire "action_counts"     → graph.py
                                                                    → data/graph_*.csv
                                                                           │
                                                                   dashboard/app.py
```

---

## Lancement

```bash
./run.sh
```

Le script vérifie les prérequis, crée le virtualenv si nécessaire, installe les dépendances, puis démarre les trois composants en parallèle avec redirection des logs dans `logs/`.

**Lancement manuel (3 terminaux) :**

```bash
# Terminal 1 — Simulateur
python -m simulator.event_producer

# Terminal 2 — Pipeline Spark
python -m pipeline.streaming

# Terminal 3 — Dashboard
streamlit run dashboard/app.py
```

Dashboard accessible sur : [http://localhost:8501](http://localhost:8501)
