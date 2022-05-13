import docker
import os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))


def airflow_build():
    """
    This function runs the build for a container with airflow processes locally.
    Turns on webserver and scheduler.
    :param dag_path: the filepath where dags live locally
    :param test_path: the filepath where the pytest script lives locally.
    :return: returns the docker container object.
    """
    DAG_PATH = f"{BASE_DIR}/dags/"
    REQUIREMENTS = f"{BASE_DIR}/requirements.txt"

    client = docker.from_env()
    client.images.pull("apache/airflow")
    running_container = client.containers.run(
        "neylsoncrepalde/airflow-docker",
        detach=True,
        ports={"8080/tcp": 8080},  # expose local port 8080 to container
        mem_limit="16g",
        mem_swappiness=100,
        nano_cpus=16,
        volumes={
            DAG_PATH: {"bind": "/usr/local/airflow/dags/", "mode": "rw"},
            REQUIREMENTS: {"bind": "/usr/local/airflow/requirements.txt", "mode": "rw"},
        },
        # platform="linux/amd64",
    )
    running_container.exec_run(
        "airflow initdb", detach=True
    )  # docker execute to initialize the airflow db
    running_container.exec_run(
        "airflow scheduler", detach=True
    )  # docker execute to start airflow scheduler
    running_container.exec_run(
        "airflow local worker", detach=True
    )  # docker execute to start airflow local worker
    running_container.exec_run(
        "pip3 install -r requirements.txt"
    )  # docker execute to install requirements
    print(
        "running container", {"service": "airflow", "container_id": running_container}
    )
