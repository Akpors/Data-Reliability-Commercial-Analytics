"""Products and customers: the slowly changing reference data of the synthetic retailer."""

from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# The customer population is fixed by the seed alone; ``as_of`` only filters it.
SIGNUP_START = date(2025, 1, 1)
SIGNUP_END = date(2027, 12, 31)

CATEGORIES = ["Home", "Kitchen", "Garden", "Toys", "Books", "Beauty", "Sport", "Electronics"]
_ADJECTIVES = ["Classic", "Compact", "Deluxe", "Eco", "Essential", "Premium", "Rustic", "Smart"]
_NOUNS = ["Lamp", "Kettle", "Planter", "Puzzle", "Notebook", "Candle", "Bottle", "Speaker"]
_COUNTRIES = ["GB", "IE", "DE", "FR", "US"]
_COUNTRY_WEIGHTS = [0.80, 0.05, 0.05, 0.05, 0.05]
_SEGMENTS = ["consumer", "vip", "trade"]
_SEGMENT_WEIGHTS = [0.80, 0.05, 0.15]


def build_products(seed: int, n: int = 150) -> pd.DataFrame:
    """The product catalogue (``raw.products`` source columns, all strings)."""
    rng = np.random.default_rng([seed, 1])
    prices = np.round(rng.uniform(2.0, 250.0, size=n), 2)
    active = rng.random(n) < 0.95
    adjectives = rng.choice(_ADJECTIVES, size=n)
    nouns = rng.choice(_NOUNS, size=n)
    categories = rng.choice(CATEGORIES, size=n)
    return pd.DataFrame(
        {
            "product_id": [f"P-{i:05d}" for i in range(1, n + 1)],
            "sku": [f"SKU-{i:05d}" for i in range(1, n + 1)],
            "product_name": [f"{a} {b}" for a, b in zip(adjectives, nouns, strict=True)],
            "category": categories,
            "list_price_gbp": [f"{p:.2f}" for p in prices],
            "is_active": ["true" if a else "false" for a in active],
        }
    )


def build_customers(seed: int, as_of: date, n_total: int = 3000) -> pd.DataFrame:
    """Customers who have signed up by ``as_of`` (``raw.customers`` source columns, strings).

    The population of ``n_total`` customers depends on ``seed`` only, with ``signup_date``
    uniform over 2025-01-01..2027-12-31, so a later ``as_of`` returns a superset of an earlier one.
    """
    rng = np.random.default_rng([seed, 2])
    fake = Faker("en_GB")
    fake.seed_instance(seed)
    span_days = (SIGNUP_END - SIGNUP_START).days + 1
    signup_offsets = rng.integers(0, span_days, size=n_total)
    countries = rng.choice(_COUNTRIES, size=n_total, p=_COUNTRY_WEIGHTS)
    segments = rng.choice(_SEGMENTS, size=n_total, p=_SEGMENT_WEIGHTS)
    names = [fake.name() for _ in range(n_total)]
    customers = pd.DataFrame(
        {
            "customer_id": [f"C-{i:06d}" for i in range(1, n_total + 1)],
            # The numeric suffix keeps emails unique whatever names Faker draws.
            "email": [f"customer{i}@example.com" for i in range(1, n_total + 1)],
            "full_name": names,
            "country": countries,
            "signup_date": [
                (SIGNUP_START + timedelta(days=int(o))).isoformat() for o in signup_offsets
            ],
            "segment": segments,
        }
    )
    # ISO dates sort lexicographically, so a string comparison is a date comparison.
    return customers[customers["signup_date"] <= as_of.isoformat()].reset_index(drop=True)
