# Simulateur d'événements : génère des interactions utilisateur/produit/vendeur
# sous forme de fichiers JSON déposés dans data/events/ toutes les 500ms.

import json
import os
import random
import time
from datetime import datetime, timezone

from faker import Faker

from config.settings import DATA_PATH

fake = Faker("fr_FR")

CATEGORIES = ["Véhicules", "Immobilier", "Électronique", "Mode", "Maison", "Loisirs", "Animaux"]

PRICE_RANGES = {
    "Véhicules":    (500.0,  25000.0),
    "Immobilier":   (300.0,  2000.0),
    "Électronique": (20.0,   2000.0),
    "Mode":         (5.0,    300.0),
    "Maison":       (10.0,   800.0),
    "Loisirs":      (5.0,    500.0),
    "Animaux":      (10.0,   400.0),
}

ACTIONS  = ["AIME", "VOUT", "ACHAT"]
# Répartition réaliste : les likes sont plus fréquents que les vues, les achats rares.
WEIGHTS  = [0.60,   0.30,   0.10]


def generate_event() -> dict:
    category = random.choice(CATEGORIES)
    low, high = PRICE_RANGES[category]
    return {
        "timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"),
        "user_id":     f"usr_{random.randint(1000, 9999)}",
        "user_city":   fake.city(),
        "product_id":  f"prod_{random.randint(1000, 9999)}",
        "product_cat": category,
        "seller_id":   f"sel_{random.randint(100, 999)}",
        "action_type": random.choices(ACTIONS, weights=WEIGHTS)[0],
        "price":       round(random.uniform(low, high), 2),
    }


def run(output_dir: str = DATA_PATH, delay: float = 0.5) -> None:
    os.makedirs(output_dir, exist_ok=True)
    print(f"Simulateur démarré → {output_dir}  (délai {delay}s)")
    while True:
        event = generate_event()
        filename = os.path.join(output_dir, f"event_{int(time.time() * 1000)}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(event, f, ensure_ascii=False)
        time.sleep(delay)


if __name__ == "__main__":
    run()
