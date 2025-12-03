from __future__ import annotations
import os
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def redis_smoke_test():
    import redis

    host = os.environ.get("REDIS_HOST", "redis")
    port = int(os.environ.get("REDIS_PORT", 6379))
    db = int(os.environ.get("REDIS_DB", 0))

    r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    key = f"smoketest:redis:{int(time.time())}"
    r.set(key, "ok", ex=60)
    val = r.get(key)
    if val != "ok":
        raise RuntimeError(f"Redis smoketest failed, expected 'ok', got {val!r}")


def neo4j_smoke_test():
    from neo4j import GraphDatabase

    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "neo4j")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    ts = int(datetime.utcnow().timestamp())

    with driver.session() as session:
        session.run("CREATE (n:SmokeTest {ts: $ts})", ts=ts)
        result = session.run("MATCH (n:SmokeTest {ts: $ts}) RETURN count(n) AS c", ts=ts)
        record = result.single()
        count = record["c"] if record else 0
        if count != 1:
            raise RuntimeError(f"Neo4j smoketest failed, expected 1, got {count}")
        # cleanup
        session.run("MATCH (n:SmokeTest {ts: $ts}) DETACH DELETE n", ts=ts)

    driver.close()


def _make_dag() -> DAG:
    with DAG(
        dag_id="integration_test_dag",
        description="Smoke tests for Redis and Neo4j connectivity",
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
        tags=["infra", "smoke"],
    ) as dag:
        redis_task = PythonOperator(
            task_id="redis_smoke_test",
            python_callable=redis_smoke_test,
        )

        neo4j_task = PythonOperator(
            task_id="neo4j_smoke_test",
            python_callable=neo4j_smoke_test,
        )

        [redis_task, neo4j_task]

    return dag


dag = _make_dag()
