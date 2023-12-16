from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_status_counts(df):
    status_list = df.select("status").distinct().rdd.flatMap(lambda x: x).collect()
    for status in status_list:
        df_status = df.filter(col("status") == status)
        count = df_status.count()
        print(f"Status: {status}, Count: {count}")

def retry_failed_jobs(failed_df): 