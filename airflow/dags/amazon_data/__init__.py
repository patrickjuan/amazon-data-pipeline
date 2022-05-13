import requests
import pandas as pd
import gzip
import os
from sqlalchemy import create_engine
from glob import glob
import hashlib
import json

URLS = {
    "meta": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta",
    "reviews": "http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews",
}
BASE_DIR = os.path.dirname(os.path.realpath(__file__))


def _create_db_connection():
    try:
        user = "user"
        passw = "password"
        host = "host.docker.internal"
        port = 5432
        db = "database"
        conn = create_engine(f"postgresql://{user}:{passw}@{host}:{port}/{db}")
        return conn
    except Exception as err:
        print("Failed to create db connection", {"error": err})
        raise err


def download_file(table: str, type: str):
    try:
        url = URLS[type]
        _logging = {"type": type, "file": table}
        print("starting download", _logging)

        response = requests.get(f"{url}_{table}.json.gz")
        file_path = f"{BASE_DIR}/{type}_{table}.json.gz"

        with open(file_path, "wb") as file:
            file.write(response.content)

        print("download suceeded!", _logging)
        return file_path
    except Exception as err:
        _logging["error"] = err
        print("error to download!", _logging)
        raise err


def read_file(file_path: str):
    def _read_json_gz(path):
        file = gzip.open(path, "rb")
        for f in file:
            yield eval(f)

    def _json_to_df(path):
        i = 0
        df = {}
        for d in _read_json_gz(path):
            df[i] = d
            i += 1
        return pd.DataFrame.from_dict(df, orient="index")

    try:
        print("reading file", {"file": file_path})
        dataframe = _json_to_df(file_path)
        os.remove(file_path)
        return dataframe
    except Exception as err:
        print(
            "failed to read file",
            {"file": file_path, "error": err},
        )
        raise err


def transform_data(df):
    try:
        print("transforming data... creating dimensional model!")

        def _create_id(string):
            return (
                int(hashlib.sha256(str(string).encode("utf-8")).hexdigest(), 16)
                % 10 ** 8
            )

        columns_to_delete = []

        # create categories table
        if "categories" in df:
            df = df.explode("categories").explode("categories")
            df["id_category"] = df["categories"].apply(lambda x: _create_id(x))
            df_category = df[["categories", "id_category"]].head(1)
            columns_to_delete = ["categories"]
        else:
            df_category = None

        # create brand table
        if "brand" in df:
            df["id_brand"] = df["brand"].apply(lambda x: _create_id(x))
            df_brand = df[["brand", "id_brand"]].head(1)
            columns_to_delete.append("brand")
        else:
            df_brand = None

        # create image table
        if "imUrl" in df:
            df_image = df[["asin", "imUrl"]]
            columns_to_delete.append("imUrl")
        else:
            df_image = None

        # create related table
        if "related" in df:
            df_related = df[["asin", "related"]]
            json_struct = json.loads(df_related.to_json(orient="records"))
            df_related = pd.json_normalize(json_struct)
            df_related = df_related.drop(columns=["related"])
            df_related.columns = [
                c.replace(".", "_") for c in df_related.columns.to_list()
            ]
            columns_to_delete.append("related")
        else:
            df_related = None

        if "salesRank" in df:
            df["salesRank"] = df["salesRank"].astype(str)

        # drop columns from product dataframe
        df = df.drop(columns=columns_to_delete)

        print("tables created with sucess!")
        return {
            "product": df,
            "category": df_category,
            "brand": df_brand,
            "image": df_image,
            "related": df_related,
        }
    except Exception as err:
        print("failed to transform data!", {"error": err})
        raise err


def load_data(data: dict):
    for d in data.items():
        try:
            table_name = d[0]
            dataframe = d[1]
            if dataframe is not None:
                print("loading data...", {"table": table_name})
                conn = _create_db_connection()
                dataframe.columns = dataframe.columns.str.lower()
                dataframe.to_sql(
                    table_name,
                    conn,
                    index=False,
                    if_exists="append",
                    method="multi",
                    schema="public",
                )
                print("table loaded with success!", {"table": table_name})
        except Exception as err:
            print("failed to load table", {"table": table_name, "error": err})
            raise err


def create_tables():
    try:
        conn = _create_db_connection()
        for file in glob(f"{BASE_DIR}/queries/*.sql"):
            fh = open(file, "r")
            query = fh.read()
            print(f"executing... {query}")
            with conn.connect() as cursor:
                cursor.execute(query)
        print("tables created with success!")
    except Exception as err:
        print("failed to create tables", {"error": err})
        raise err
