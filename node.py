import random
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count
from rich import print
import os

java_home = os.environ.get("JAVA_HOME")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Inicializar Spark
spark = (
    SparkSession.builder.appName("Node Error Detection and Recovery")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)

# Crear DF de nodos
data = [(f"Nodo_{i}", "OK", 0, 0) for i in range(1, 11)]
nodes_df = spark.createDataFrame(data, ["nodo", "status", "latency", "tasks"])

# Probabilidad de fallo de nodos
fail_prob = {f"Nodo_{i}": random.uniform(0.1, 0.3) for i in range(1, 11)}
critical_fail_nodes = random.sample(list(fail_prob.keys()), 2)
for node in critical_fail_nodes:
    fail_prob[node] = 1.0


def assign_tasks():
    tasks = [
        (
            i,
            random.choice(list(fail_prob.keys())),
            random.randint(500, 5000),
            "Pendiente",
        )
        for i in range(10)
    ]
    return spark.createDataFrame(tasks, ["id", "nodo_asignado", "latency", "estado"])


def process_tasks(tasks_df):
    global nodes_df

    # Fallos
    fail_udf = when(
        col("nodo_asignado").isin(
            [n for n, p in fail_prob.items() if random.random() < p]
        ),
        "Fallido",
    ).otherwise("Procesando")

    tasks_df = tasks_df.withColumn("estado", fail_udf)

    # Actualizar estado de nodos
    failed_nodes = (
        tasks_df.filter(col("estado") == "Fallido").select("nodo_asignado").distinct()
    )
    nodes_df = (
        nodes_df.join(
            failed_nodes, nodes_df.nodo == failed_nodes.nodo_asignado, "left_outer"
        )
        .withColumn(
            "status",
            when(col("nodo_asignado").isNotNull(), "FALLA").otherwise(col("status")),
        )
        .drop("nodo_asignado")
    )

    # Reasignación de tareas
    failed_tasks = tasks_df.filter(col("estado") == "Fallido")
    reassigned_tasks = 0

    aviable_nodes = (
        nodes_df.filter(col("status") == "OK")
        .select("nodo")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    if aviable_nodes:
        reassigned_tasks = failed_tasks.count()
        tasks_df = tasks_df.withColumn(
            "nodo_asignado",
            when(col("estado") == "Fallido", random.choice(aviable_nodes)).otherwise(
                col("nodo_asignado")
            ),
        )
        tasks_df = tasks_df.withColumn(
            "estado",
            when(col("estado") == "Fallido", "Reasignado").otherwise(col("estado")),
        )

    processed = tasks_df.filter(col("estado").isin(["Procesado", "Reasignado"])).count()
    failed = tasks_df.filter(col("estado") == "Fallido").count()
    return processed, failed, reassigned_tasks


def print_metrics(iteration, processed, failed, reassigned, nodes_df):
    recovery_rate = (reassigned / failed * 100) if failed > 0 else 0
    failed_nodes = [
        row.nodo for row in nodes_df.filter(col("status") == "FALLA").collect()
    ]

    print(f"\n[bold]Iteración {iteration}[/bold]")
    print(f"Tareas procesadas: [green]{processed}[/green]")
    print(f"Tareas fallidas: [red]{failed}[/red]")
    print(f"Tareas reasignadas: [yellow]{reassigned}[/yellow]")
    print(f"Recuperación: [cyan]{recovery_rate:.2f}%[/cyan]")
    print(f"Nodos en falla: [red]{failed_nodes}[/red]\n")


for i in range(10):
    tasks_df = assign_tasks()
    processed, failed, reassigned = process_tasks(tasks_df)
    print_metrics(i + 1, processed, failed, reassigned)
    time.sleep(5)

spark.stop()
