import random
from pyspark.sql import SparkSession
import os

java_home = os.environ.get("JAVA_HOME")

spark = SparkSession.builder.appName("Node Error Detection and Recovery").getOrCreate()

nodes = {f"Nodo_{i}": {"status": "OK", "latency": 0, "tasks": 0} for i in range(1, 11)}
fail_prob = {node: random.uniform(0.1, 0.3) for node in nodes}
critical_fail_node = random.sample(list(nodes.keys()), 2)

for node in critical_fail_node:
    fail_prob[node] = 1.0


def asign_tasks():
    tasks = []
    for _ in range(10):
        node = random.choice(list(nodes.keys()))
        latency = random.randint(500, 5000)
        tasks.append((node, latency))

    return tasks


def process_tasks(tasks):
    failed_tasks = []
    processed_tasks = []
    reassinged_tasks = 0

    for node, latency in tasks:
        if random.random() < fail_prob[node]:
            nodes[node]["status"] = "FALLA"
            failed_tasks.append((node, latency))
        else:
            nodes[node]["latency"] = latency
            if latency > 4000:
                nodes[node]["status"] = "LENTO"
