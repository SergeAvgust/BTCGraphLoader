# BTC Transactions loader

> 

## Requirements

- Docker
  - [docker-compose](https://docs.docker.com/compose/install/)

## Setup environment

Copy the sample `.env` files to the right location and modify values if needed.

```shell script
cp ./docker/redis/.env.sample ./docker/redis/.env
cp ./docker/api/.env.sample ./docker/api/.env
cp ./docker/worker/.env.sample ./docker/worker/.env
```

## Run app

1. Run command ```docker compose up``` to start up the Redis, Neo4j and application/worker instances.
2. Navigate to the [http://localhost:8000/docs](http://localhost:8000/docs)
3. POST /download/ starts download of requested blockchair dump url
4. GET /transactions/{address} returns all transactions connected to requested address
5. Celery beat attempts to launch download of new dumps at 00:00 UTC
6. Celery beat and Redis both take part in implementation of non-concurrent queue (due to blockchair API limitations)

## Run application/worker without Docker?

### Requirements/dependencies

- Python >= 3.12
  - [poetry](https://python-poetry.org/docs/#installation)

> The Redis and Neo4j can be started with ```docker compose -f docker-compose-services.yml up```

### Install dependencies

Execute the following command: ```poetry install --dev```

### Run FastAPI app and Celery worker app

1. Start the FastAPI web application with ```poetry run hypercorn app.main:app --reload```.
2. Start the celery worker with command 
```poetry run celery -A app.worker.celery_worker worker -l info ```
3. Start the celery beat with command
```poetry run celery -A app.worker.celery_worker beat -l info```
