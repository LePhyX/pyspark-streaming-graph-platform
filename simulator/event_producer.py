import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from config.settings import DATA_PATH


USERS = [f"u{i}" for i in range(1, 11)]
PRODUCTS = [f"p{i}" for i in range(1, 6)]
SELLERS = [f"s{i}" for i in range(1, 4)]
USER_ACTIONS = ["AIME", "VOUT", "ACHAT"]


def generate_event() -> dict:
    if random.random() < 0.8:
        return {
            "user_id": random.choice(USERS),
            "product_id": random.choice(PRODUCTS),
            "seller_id": random.choice(SELLERS),
            "action": random.choice(USER_ACTIONS),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    return {
        "user_id": None,
        "product_id": random.choice(PRODUCTS),
        "seller_id": random.choice(SELLERS),
        "action": "PROPOSE",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def produce_events(n: int = 10, interval: float = 1.0) -> None:
    os.makedirs(DATA_PATH, exist_ok=True)
    for _ in range(n):
        event = generate_event()
        path = os.path.join(DATA_PATH, f"{uuid.uuid4()}.json")
        with open(path, "w") as f:
            json.dump(event, f)
        time.sleep(interval)


if __name__ == "__main__":
    produce_events(n=50, interval=0.5)
