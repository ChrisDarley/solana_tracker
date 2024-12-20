import requests
from snowflake.snowpark.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)
from include.eczachly.snowflake_queries import get_snowpark_session


def load_tokens(execution_time):
    st_key = "your api key here"
    st_url = "https://data.solanatracker.io"

    # Endpoint for latest graduated tokens
    endpoint = "/tokens/multi/graduated"

    # Construct the full URL
    url = f"{st_url}{endpoint}"

    # Headers with API key for authentication
    headers = {"Content-Type": "application/json", "x-api-key": st_key}

    r = requests.get(url, headers=headers)  # , params=params)
    if r.status_code != 200:
        print("api failed with status code:", r.status_code)
        return
        # records = get_token_info(r.json())

    # function to parse request response
    def get_token_info(response_json):
        records = []
        for token in response_json:
            if len(token["pools"]) == 2:
                token_name = token["token"]["name"]
                token_symbol = token["token"]["symbol"]
                token_mint = token["token"]["mint"]

                # Safely access pool_1 keys
                pool_1 = token["pools"][1]
                pool_1_id = pool_1.get("poolId", None)
                pool_1_market = pool_1.get("market", None)
                pool_1_created_at = pool_1.get("createdAt", None)
                pool_1_last_updated = pool_1.get("lastUpdated", None)

                # Safely access pool_2 keys
                pool_2 = token["pools"][0]
                pool_2_id = pool_2.get("poolId", None)
                pool_2_market = pool_2.get("market", None)
                pool_2_created_at = pool_2.get("createdAt", None)
                pool_2_last_updated = pool_2.get("lastUpdated", None)

                # Create the record
                record = {
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "token_mint": token_mint,
                    "pool_1_id": pool_1_id,
                    "pool_1_market": pool_1_market,
                    "pool_1_created_at": pool_1_created_at,
                    "pool_1_last_updated": pool_1_last_updated,
                    "pool_2_id": pool_2_id,
                    "pool_2_market": pool_2_market,
                    "pool_2_created_at": pool_2_created_at,
                    "pool_2_last_updated": pool_2_last_updated,
                    "execution_time": execution_time,
                }
                records.append(record)

            else:
                pass

        return records

    records = get_token_info(r.json())

    if not records:
        print("No records found to load.")
        return

    table_name = "chrisdarley.src_distinct_tokens"

    create_src_distinct_tokens_ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        token_name VARCHAR,
        token_symbol VARCHAR,
        token_mint VARCHAR,
        pool_1_id VARCHAR,
        pool_1_market VARCHAR,
        pool_1_created_at INTEGER,
        pool_1_last_updated INTEGER,
        pool_2_id VARCHAR,
        pool_2_market VARCHAR,
        pool_2_created_at INTEGER,
        pool_2_last_updated INTEGER,
        execution_time DATETIME
    );
    """

    # Define schema
    schema = StructType(
        [
            StructField("token_name", StringType()),
            StructField("token_symbol", StringType()),
            StructField("token_mint", StringType()),
            StructField("pool_1_id", StringType()),
            StructField("pool_1_market", StringType()),
            StructField("pool_1_created_at", IntegerType()),
            StructField("pool_1_last_updated", IntegerType()),
            StructField("pool_2_id", StringType()),
            StructField("pool_2_market", StringType()),
            StructField("pool_2_created_at", IntegerType()),
            StructField("pool_2_last_updated", IntegerType()),
            StructField("execution_time", TimestampType()),
        ]
    )

    session = get_snowpark_session()  # schema="chrisdarleycapstone")
    session.sql("CREATE SCHEMA IF NOT EXISTS chrisdarley").collect()

    session.sql(create_src_distinct_tokens_ddl).collect()

    df = session.create_dataframe(records, schema=schema)
    df.write.mode("append").save_as_table(table_name)
    print(f"Loaded {len(records)} records into {table_name}")
