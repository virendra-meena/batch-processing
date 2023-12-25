import subprocess

SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/"

def stop_spark_master():
    print("Stopping Spark Master...")
    subprocess.Popen(["sbin/stop-master.sh"], cwd=SPARK_HOME)

def stop_spark_worker(worker_id):
    print(f"Stopping Spark Worker {worker_id}...")
    subprocess.Popen(["sbin/stop-worker.sh"], cwd=SPARK_HOME)

if __name__ == "__main__":
    # Stop Spark Master
    stop_spark_master()

    # Stop Spark Workers
    stop_spark_worker(1)

    print("Spark Cluster stopped successfully.")
