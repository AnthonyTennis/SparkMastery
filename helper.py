from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def get_status_counts(df):
    status_list = df.select("status").distinct().rdd.flatMap(lambda x: x).collect()
    for status in status_list:
        df_status = df.filter(col("status") == status)
        count = df_status.count()
        print(f"Status: {status}, Count: {count}")

def get_successful_jobs(df):
    return df.filter(col("status") == "Successful")

def get_failed_jobs(df):
    return df.filter(col("status") == "Failed")

def get_pending_jobs(df):
    return df.filter(col("status") == "Pending")

def get_in_progress_jobs(df):
    return df.filter(col("status") == "In Progress")

def count_pending_in_progress_jobs(df):
    count = df.filter(
        (col("status").isin("Pending", "In Progress"))
    ).count()

    return count

def retry_failed_jobs(failed_df): 
    return failed_df.withColumn("status", when(col("status") == "Failed", "Pending").otherwise(col("status")))

def filter_jobs_by_team(df, team_name):
    filtered_df = df.filter(df.teamName == team_name)
    return filtered_df