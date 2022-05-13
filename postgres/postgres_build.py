import docker


def postgres_build():
    client = docker.from_env()
    client.images.pull("postgres")
    running_container = client.containers.run(
        "postgres",
        detach=True,
        ports={"5432/tcp": 5432},
        platform="linux/amd64",
        environment={
            "POSTGRES_USER": "user",
            "POSTGRES_DB": "database",
            "POSTGRES_PASSWORD": "password",
        },
    )
    print(
        "running container", {"service": "postgres", "container_id": running_container}
    )
