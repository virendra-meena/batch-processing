import os
import subprocess
import time

host = "Priyas-MacBook-Air.local"
port = "7077"
SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/"

def start_spark_master():
    print("Starting Spark Master...")
    subprocess.Popen(["sbin/start-master.sh"], cwd=SPARK_HOME)
    time.sleep(2)  # Wait for the master to start

def start_spark_worker(worker_id):
    print(f"Starting Spark Worker {worker_id}...")
    subprocess.Popen(["sbin/start-worker.sh", f"spark://{host}:{port}"], cwd=SPARK_HOME)
    time.sleep(2)  # Wait for the worker to start

if __name__ == "__main__":
    # Set the SPARK_HOME environment variable to the Spark installation directory
    spark_home = "/opt/homebrew/Cellar/apache-spark/3.5.0/libexec"
    os.environ["SPARK_HOME"] = spark_home

    # Start Spark Master
    start_spark_master()

    # Start two Spark Workers
    start_spark_worker(1)

    print("Spark Cluster started successfully.")
