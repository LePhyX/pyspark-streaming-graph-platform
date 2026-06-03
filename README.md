# PySpark Streaming Graph Platform

Real-time streaming pipeline simulating user-seller-product interactions on a marketplace (LeBonCoin-style), processed with PySpark Structured Streaming and visualized as a dynamic graph using GraphFrames.

## Stack

- Python 3.10+
- PySpark 3.5+
- GraphFrames 0.8.3
- Streamlit
- NetworkX / PyVis

## Prerequisites

- Java 11+
- Python 3.10+
- pip

## Installation

```bash
pip install -r requirements.txt
```

## Launch

```bash
# Terminal 1 — Event simulator
python -m simulator.event_producer

# Terminal 2 — Spark streaming pipeline
python -m pipeline.streaming

# Terminal 3 — Dashboard
streamlit run dashboard/app.py
```

## Project Structure

```
pyspark-streaming-graph-platform/
├── README.md
├── requirements.txt
├── .gitignore
├── simulator/
│   ├── __init__.py
│   └── event_producer.py       # Infinite JSON event stream
├── pipeline/
│   ├── __init__.py
│   ├── spark_session.py        # SparkSession factory
│   ├── schema.py               # Strict event schema
│   ├── streaming.py            # Structured Streaming + windowing
│   └── graph.py                # GraphFrames model
├── dashboard/
│   ├── __init__.py
│   └── app.py                  # Streamlit dashboard (auto-refresh 5s)
├── config/
│   └── settings.py             # Centralized constants
├── tests/
│   ├── test_simulator.py
│   ├── test_pipeline.py
│   └── test_graph.py
└── report/
    └── rapport_technique.pdf
```

## Event Schema

| Field        | Type      | Example              |
|--------------|-----------|----------------------|
| timestamp    | Timestamp | 2026-06-06T14:32:01  |
| user_id      | String    | usr_9482             |
| user_city    | String    | Paris                |
| product_id   | String    | prod_5501            |
| product_cat  | String    | Véhicules            |
| seller_id    | String    | sel_0214             |
| action_type  | String    | AIME / VOUT / ACHAT  |
| price        | Double    | 450.00               |
