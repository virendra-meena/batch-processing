from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySQLReader").config(
    "spark.jars", "/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/jars/mysql-connector-j-8.2.0.jar").getOrCreate()

host = "localhost"
port = "3306"
database = "test_subscription_db"
table = "subscription"
username = "root"
password = "salesforce123"
path_to_csv_file = "spark_processed_data.csv"

jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"

properties = {"user": "root", "password": "salesforce123",
              "driver": "com.mysql.jdbc.Driver"}

df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f"{table}").option(
    "user", f"{username}").option("password", f"{password}").load()

df.show()
df.write.mode("overwrite").csv(path_to_csv_file, header=True)

spark.stop()
