from redis import Redis
import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load MCC codes once at the global scope
mcc_df = pd.read_csv(Path("dataset") / "mcc_codes.csv")

REDIS_HOST = "localhost"
REDIS_PORT = 6379
get_redis = Redis(host=REDIS_HOST, port=REDIS_PORT)


def _enrich_transaction(
    transaction: Dict[str, Any], database: pd.DataFrame, column: str
) -> Dict[str, Any]:
    """Enrich the transaction with MCC data."""
    try:
        transaction_df = pd.DataFrame([transaction])
        transaction_df["User"] = transaction_df["User"].apply(lambda x: "User" + str(x))

        customer_profile = get_redis.get(transaction_df["User"].values[0])
        customer_profile = json.loads(customer_profile)
        CP_df = pd.DataFrame([customer_profile])
        CP_df["User"] = transaction_df["User"]

        transaction_CP = pd.merge(transaction_df, CP_df, on="User", how="left")
        enriched_transaction = pd.merge(
            transaction_CP, database, on=column, how="left"
        ).to_dict(orient="records")[0]

        return enriched_transaction
    except Exception as e:
        logger.error(f"Error enriching transaction: {e}")
        return transaction
