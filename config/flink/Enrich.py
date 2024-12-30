from redis import Redis
import pandas as pd
import json

# Load MCC codes once at the global scope to avoid reloading in every operation
mcc_df = pd.read_csv("dataset/mcc_codes.csv")

REDIS_HOST = "localhost"
REDIS_PORT = 6379
get_redis = Redis(host=REDIS_HOST, port=REDIS_PORT)


def _enrich_transaction(transaction):
    """Enrich the transaction with MCC data."""
    try:
        # convert transaction into DataFrame
        transaction_df = pd.DataFrame([transaction])

        # convert user column from user:0 into user0
        transaction_df["User"] = transaction_df["User"].apply(lambda x: "User" + str(x))
        # read data from redis for sepecific user
        customer_profile = get_redis.get(transaction_df["User"].values[0])
        customer_profile = json.loads(customer_profile)
        CP_df = pd.DataFrame([customer_profile])
        # add user column to customer profile
        CP_df["User"] = transaction_df["User"]

        # merge customer profile with rides
        transaction_CP = pd.merge(transaction_df, CP_df, on="User", how="left")

        enriched_transaction = pd.merge(
            transaction_CP, mcc_df, on="MCC", how="left"
        ).to_dict(orient="records")[0]

        # print(enriched_transaction.keys())

        return enriched_transaction

    except Exception as e:
        print(f"Error enriching transaction: {e}")
        return transaction
