from airflow.airflow_build import airflow_build
from postgres.postgres_build import postgres_build


def build():
    print("BUILDING SERVICES...")
    airflow_build()
    postgres_build()
    print("SERVICES UP...")
    print(
        "airflow", {"host": "localhost:8080", "user": "airflow", "password": "airflow"}
    )
    print(
        "postgres",
        {
            "host": "localhost",
            "port": 5432,
            "database": "database",
            "username": "user",
            "password": "password",
        },
    )


if __name__ == "__main__":
    build()
