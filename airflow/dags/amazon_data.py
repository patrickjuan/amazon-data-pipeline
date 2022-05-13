from datetime import datetime

from airflow import DAG
from airflow.decorators import task

import os
from datetime import date

from amazon_data import (
    download_file,
    read_file,
    transform_data,
    load_data,
    create_tables,
)

today = date.today()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

FILES_NAMES = [
    "Books",
    "Electronics",
    "Movies_and_TV",
    "CDs_and_Vinyl",
    "Clothing_Shoes_and_Jewelry",
    "Home_and_Kitchen",
    "Kindle_Store",
    "Sports_and_Outdoors",
    "Cell_Phones_and_Accessories",
    "Health_and_Personal_Care",
    "Toys_and_Games",
    "Video_Games",
    "Tools_and_Home_Improvement",
    "Beauty",
    "Apps_for_Android",
    "Office_Products",
    "Pet_Supplies",
    "Automotive",
    "Grocery_and_Gourmet_Food",
    "Patio_Lawn_and_Garden",
    "Baby",
    "Digital_Music",
    "Musical_Instruments",
    "Amazon_Instant_Video",
]

with DAG(
    dag_id="amazon_data_pipeline",
    start_date=datetime(2022, 1, 1),
    schedule_interval="0 */8 * * *",
) as dag:

    @task
    def dummy_start_task():
        pass

    task_meta = []
    for file in FILES_NAMES:

        @task(task_id=f"meta_{file}")
        def meta(file):
            files = download_file(file, "meta")
            dataframe = read_file(files)
            data_tables = transform_data(dataframe)
            load_data(data_tables)

        task_meta.append(meta(file))

    task_reviews = []
    for file in FILES_NAMES:

        @task(task_id=f"reviews_{file}")
        def reviews(file):
            files = download_file(file, "reviews")
            dataframe = read_file(files)
            load_data({"reviews": dataframe})

        task_reviews.append(reviews(file))

    @task
    def task_create_tables():
        create_tables()

    @task
    def dummy_finish_task():
        pass

    @task
    def dummy_continue_task():
        pass

    start_task_ = dummy_start_task()
    finish_task = dummy_finish_task()
    continue_task = dummy_continue_task()
    creates_task = task_create_tables()
    (
        start_task_
        >> creates_task
        >> task_meta
        >> continue_task
        >> task_reviews
        >> finish_task
    )
