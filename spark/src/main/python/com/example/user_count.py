"""
command to submit this job -
spark-submit --master spark://Priyas-MacBook-Air.local:7077 spark/src/main/python/com/example/user_count.py
"""

from pyspark.sql import SparkSession

# Set the master URL to connect to the remote Spark Master
master_url = "spark://Priyas-MacBook-Air.local:7077"  # Update the port if necessary

# Configure Spark with necessary JARs
spark = SparkSession.builder.appName("UserCountApp").config(
    "spark.jars", "/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/jars/mysql-connector-j-8.2.0.jar"
).config("spark.master", master_url).getOrCreate()

# MySQL database connection parameters
host = "localhost"
port = "3306"
database = "test_subscription_db"
table = "subscription"
username = "root"
password = "salesforce123"
path_to_csv_file = "spark_processed_data.csv"

# JDBC URL for MySQL
jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"

# Properties for the MySQL connection
properties = {"user": username, "password": password, "driver": "com.mysql.jdbc.Driver"}

# Read data from MySQL
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", table).option(
    "user", username).option("password", password).load()

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Display the DataFrame
df.show()

# Define a window specification
window_spec = Window.partitionBy("user_id")

# Perform transformations to get the count of subscriptions per user_id
result_df = df.withColumn("subscription_count", F.count("subscription_id").over(window_spec))

# Select relevant columns
result_df = result_df.select("user_id", "subscription_count").distinct()

# Show the result
result_df.show()

# Write data to CSV
result_df.write.mode("overwrite").csv(path_to_csv_file, header=True)

# Stop the Spark session
spark.stop()
