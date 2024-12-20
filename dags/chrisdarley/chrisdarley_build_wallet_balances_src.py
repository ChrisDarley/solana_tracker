import requests
import pandas as pd
import itertools
from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)
from include.eczachly.snowflake_queries import get_snowpark_session

session = get_snowpark_session()

qn_key = "placeholder"
qn_url = "placeholder"


def get_all_wallet_balances(execution_time):
    """For each token mint address, fetches the balances held of that token by every wallet"""

    def get_unique_wallets():
        unique_wallets = session.sql(
            "SELECT DISTINCT token_mint FROM chrisdarley.src_distinct_tokens LIMIT 150"
        ).collect()
        print([wallet[0] for wallet in unique_wallets])
        unique_wallets = [wallet[0] for wallet in unique_wallets]
        return unique_wallets

    def fetch_owners(mint_address):
        url = qn_url
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {
                    "encoding": "jsonParsed",
                    "filters": [
                        {"dataSize": 165},
                        {"memcmp": {"offset": 0, "bytes": mint_address}},
                    ],
                },
            ],
        }
        r = requests.post(url, json=payload)
        mint_addresses = r.json()
        return mint_addresses

    def get_data(mint_addresses):
        results = []
        for mint_address in mint_addresses:
            data = fetch_owners(mint_address)
            print(data)
            result = data["result"]
            results.append(result)
        return results

    def parse_data(results):
        results_flat = list(itertools.chain.from_iterable(results))
        df_parsed = (
            pd.json_normalize(data=results_flat)
            .rename(
                {
                    "account.data.parsed.info.mint": "mint_address",
                    "account.data.parsed.info.owner": "owner_address",
                    "account.data.parsed.info.tokenAmount.amount": "amount",
                    "account.data.parsed.info.tokenAmount.decimals": "decimals",
                },
                axis=1,
            )
            .loc[:, ["mint_address", "owner_address", "amount", "decimals"]]
            .assign(execution_time=execution_time)
        )
        return df_parsed

    def append_data(df_parsed):

        table_name = "chrisdarley.src_transactions"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            mint_address VARCHAR,
            owner_address VARCHAR,
            amount NUMBER,
            decimals NUMBER,
            execution_time TIMESTAMP_NTZ
        )
        """
        session.sql(create_table_query).collect()
        df = session.create_dataframe(df_parsed)
        df.write.mode("append").save_as_table(table_name)
        print("appended wallet data")

    def run_pipeline():
        unique_wallets = get_unique_wallets()
        mint_addresses = fetch_owners(unique_wallets)
        results = get_data(mint_addresses)
        df_parsed = parse_data(results)
        append_data(df_parsed)

    run_pipeline()
