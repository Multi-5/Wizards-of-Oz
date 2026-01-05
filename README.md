# DataEng 2024 Template Repository

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students: **[To be assigned]**

### Abstract

## Datasets Description 

## Queries 

## Requirements

## Note for Students

* Clone the created repository offline;
* Add your name and surname into the Readme file and your teammates as collaborators
* Complete the field above after project is approved
* Make any changes to your repository according to the specific assignment;
* Ensure code reproducibility and instructions on how to replicate the results;
* Add an open-source license, e.g., Apache 2.0;
* README is automatically converted into pdf


## Local setup

The project ships with an Apache Airflow + Postgres + Neo4j stack defined in `airflow/docker-compose.yml`.

1. Ensure Docker Desktop (or a compatible Docker Engine) is running.
2. From the `airflow/` directory run `docker compose up --build` to start all services.
3. Visit http://localhost:8082 to access the Airflow UI (default credentials `admin`/`admin`).

### Notes on the metadata database

* The Airflow container expects a Postgres database named `airflow`. A bootstrap script under `airflow/db/init/00-create-databases.sql` now creates both the `airflow` metadata database and the `warehouse` analytics database the first time the Postgres volume is initialized.
* If you previously ran the stack before this script existed, remove the old Postgres volume so the initializer can run:

```bash
cd airflow
docker compose down -v
docker compose up --build
```

This guarantees every teammate starts with the same schema state and avoids the `database "airflow" does not exist` error during `airflow db check`.

