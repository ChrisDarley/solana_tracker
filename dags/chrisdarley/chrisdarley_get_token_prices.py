import requests
import itertools
import time
from snowflake.snowpark.functions import flatten, col
from snowflake.snowpark.types import VariantType
from snowflake.snowpark.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
)
from include.eczachly.snowflake_queries import get_snowpark_session


def get_tokens():
    session = get_snowpark_session()
    df_seven = session.table("CHRISDARLEY.DIM_SEVEN_DAY_TOKENS")
    tokens_df = df_seven.select("TOKEN_MINT")
    tokens = [row["TOKEN_MINT"] for row in tokens_df.collect()]

    return tokens


def get_token_list(tokens):
    """break down tokens list into chunks of 100, which
    is the max length the api allows per request"""

    def divide_list(l, n):
        """generator which divide list l into sublists
        of length n (last list may be shorter)"""
        for i in range(0, len(l), n):
            yield l[i : i + n]

    token_lists = list(divide_list(tokens, 100))

    return token_lists


def get_prices(token_lists):
    """get prices for each token in token_lists sublists"""
    st_key = "api_key_placeholder"
    st_url = "https://data.solanatracker.io"
    endpoint = "/price/multi"

    # Construct the full URL
    url = f"{st_url}{endpoint}"

    # Headers with API key for authentication
    headers = {"Content-Type": "application/json", "x-api-key": st_key}

    results = []
    for tokens in token_lists:
        r = requests.post(url, headers=headers, json={"tokens": tokens})
        if r.status_code != 200:
            print("api failed with status code:", r.status_code)
            time.sleep(0.5)
            # try resending request 1 time if request fails
            r = requests.post(url, headers=headers, json={"tokens": tokens})
        if r.status_code == 200:
            results.append(r.json())
        time.sleep(0.2)

    return results


def parse_results(results, execution_time):
    """parse results into a snowpark table"""
    unnested_results = {k: v for d in results for k, v in d.items()}
    parsed_records = []
    for token_mint, attributes in unnested_results.items():
        if all(
            key in attributes
            for key in ["price", "priceQuote", "liquidity", "marketCap", "lastUpdated"]
        ):

            row = (
                token_mint,
                attributes["price"],
                attributes["priceQuote"],
                attributes["liquidity"],
                attributes["marketCap"],
                attributes["lastUpdated"],
                execution_time,
            )
            parsed_records.append(row)
        else:
            (print(type(token_mint), type(attributes), token_mint, attributes))

    schema = StructType(
        [
            StructField("token_mint", StringType()),
            StructField("price", DoubleType()),
            StructField("priceQuote", DoubleType()),
            StructField("liquidity", DoubleType()),
            StructField("marketCap", DoubleType()),
            StructField("lastUpdated", LongType()),
            StructField("execution_time", TimestampType()),
        ]
    )
    session = get_snowpark_session()
    df = session.create_dataframe(parsed_records, schema)
    df.show()
    table_name = "CHRISDARLEY.SRC_TOKEN_PRICES"
    df.write.mode("append").save_as_table(table_name)


def run_pipeline(execution_time):
    tokens = get_tokens()
    token_lists = get_token_list(tokens)
    results = get_prices(token_lists)
    parse_results(results, execution_time)
