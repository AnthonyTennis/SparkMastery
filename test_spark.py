import csv
import random
from pyspark.sql import SparkSession
import helper

# Sample data for testing
data = [
    ["Job1", "JuiceBox", "Completed", "2020-01-01", "2020-01-10"],
    ["Job2", "JuiceBox", "Failed", "2020-01-02", "2020-01-11"],
    ["Job3", "MindOvermatter", "Pending", "2020-01-03", ""],
    ["Job4", "Distributo", "In Progress", "2020-01-04", ""],
    ["Job5", "AndersonSquad", "Completed", "2020-01-05", "2020-01-15"],
    ["Job6", "AndersonSquad", "Failed", "2020-01-06", "2020-01-16"]
]

with open('test_jobs_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['jobId', 'sendingTeam', 'status', 'startDate', 'endDate'])
    writer.writerows(data)

# Initialize Spark session for testing
spark = SparkSession.builder.appName("TestJobFunctions").getOrCreate()

# Read the test CSV file into a DataFrame
df = spark.read.csv('test_jobs_data.csv', header=True, inferSchema=True)
# Test for get_successful_jobs
def test_get_successful_jobs():
    result_df = helper.get_successful_jobs(df)
    expected_job_ids = ['Job1', 'Job5']
    actual_job_ids = [row['jobId'] for row in result_df.collect()]
    assert set(expected_job_ids) == set(actual_job_ids), "get_successful_jobs test failed"

# Test for get_failed_jobs
def test_get_failed_jobs():
    result_df = helper.get_failed_jobs(df)
    expected_job_ids = ['Job2', 'Job6']
    actual_job_ids = [row['jobId'] for row in result_df.collect()]
    assert set(expected_job_ids) == set(actual_job_ids), "get_failed_jobs test failed"

# Test for get_pending_jobs
def test_get_pending_jobs():
    result_df = helper.get_pending_jobs(df)
    expected_job_ids = ['Job3']
    actual_job_ids = [row['jobId'] for row in result_df.collect()]
    assert set(expected_job_ids) == set(actual_job_ids), "get_pending_jobs test failed"

# Test for get_in_progress_jobs
def test_get_in_progress_jobs():
    result_df = helper.get_in_progress_jobs(df)
    expected_job_ids = ['Job4']
    actual_job_ids = [row['jobId'] for row in result_df.collect()]
    assert set(expected_job_ids) == set(actual_job_ids), "get_in_progress_jobs test failed"

# Test for count_pending_in_progress_jobs
def test_count_pending_in_progress_jobs():
    count = helper.count_pending_in_progress_jobs(df)
    expected_count = 2
    assert count == expected_count, "count_pending_in_progress_jobs test failed"

# Test for count_jobs_by_team
def test_count_jobs_by_team():
    result_df = helper.count_jobs_by_team(df)
    result = {row['sendingTeam']: row['count'] for row in result_df.collect()}
    expected = {'JuiceBox': 2, 'MindOvermatter': 1, 'Distributo': 1, 'AndersonSquad': 2}
    assert result == expected, "count_jobs_by_team test failed"

# Test for retry_failed_jobs
def test_retry_failed_jobs():
    failed_df = helper.get_failed_jobs(df)
    retried_df = helper.retry_failed_jobs(failed_df)
    for row in retried_df.collect():
        assert row['status'] == 'Pending', "retry_failed_jobs test failed"

# Test for filter_jobs_by_team
def test_filter_jobs_by_team():
    team_name = 'JuiceBox'
    result_df = helper.filter_jobs_by_team(df, team_name)
    for row in result_df.collect():
        assert row['sendingTeam'] == team_name, "filter_jobs_by_team test failed"

def test_jobs_completed_in_range():
    result_df = helper.jobs_completed_in_range(df, '2020-01-01', '2020-12-31')
    expected_job_ids = ['Job1', 'Job5']
    actual_job_ids = [row['jobId'] for row in result_df.collect()]
    assert set(expected_job_ids) == set(actual_job_ids), "jobs_completed_in_range test failed"

test_get_successful_jobs()
test_get_failed_jobs()
test_get_pending_jobs()
test_get_in_progress_jobs()
test_count_pending_in_progress_jobs()
test_count_jobs_by_team()
test_retry_failed_jobs()
test_filter_jobs_by_team()
test_jobs_completed_in_range()
print("All tests succeeded")

spark.stop()

