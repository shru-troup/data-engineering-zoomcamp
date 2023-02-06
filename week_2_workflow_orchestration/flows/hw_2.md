Hw 2 
#1. Green taxi data - Number of rows

```@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """This is the main ETL flow """
    color = 'green'
    year = '2020'
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz" 
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
```
Output:
congestion_surcharge            float64
dtype: object
18:34:45.554 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
18:34:45.601 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
18:34:45.644 | INFO    | Flow run 'almond-rook' - Created task run 'write_local-f322d1be-0' for task 'write_local'
18:34:45.645 | INFO    | Flow run 'almond-rook' - Executing 'write_local-f322d1be-0' immediately...
18:34:47.160 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
18:34:47.205 | INFO    | Flow run 'almond-rook' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
18:34:47.207 | INFO    | Flow run 'almond-rook' - Executing 'write_gcs-1145c921-0' immediately...
18:34:47.352 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'dtc_data_lake_inner-bonus-375206'.
18:34:47.495 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/green/green_tripdata_2020-01.parquet') to the bucket 'dtc_data_lake_inner-bonus-375206' path 'data/green/green_tripdata_2020-01.parquet'.
18:34:47.695 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
18:34:47.734 | INFO    | Flow run 'almond-rook' - Finished in state Completed('All states completed.')


#2. Scheduling with Cron: 0 5 1 * *

prefect deployment build parametrized_flow_shru.py:etl_parent_flow -n etl2 --cron "0 5 1 * *" -a
Found flow 'etl-parent-flow'
Deployment YAML created at 
'/home/shrutika/workspace/data-engineering-zoomcamp/week_2_workflow_orchestration/flows/03_deployments/etl_parent_flow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this warning.
Deployment 'etl-parent-flow/etl2' successfully created with id '85cafd43-42ff-4ae8-a299-9c7dd05468d9'.

#3. Loading data to BigQuery
14851920


~/workspace/data-engineering-zoomcamp/week_2_workflow_orchestration/flows/02_gcp$ python etl_gcs_to_bq_shru.py 
19:02:36.502 | INFO    | prefect.engine - Created flow run 'platinum-armadillo' for flow 'etl-parent-flow'
19:02:36.825 | INFO    | Flow run 'platinum-armadillo' - Created subflow run 'energetic-panda' for flow 'etl-web-to-gcs'
19:02:36.958 | INFO    | Flow run 'energetic-panda' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
19:02:36.959 | INFO    | Flow run 'energetic-panda' - Executing 'fetch-b4598a4a-0' immediately...
19:02:36.999 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Cached(type=COMPLETED)
19:02:52.033 | INFO    | Flow run 'energetic-panda' - Created task run 'clean-b9fd7e03-0' for task 'clean'
19:02:52.034 | INFO    | Flow run 'energetic-panda' - Executing 'clean-b9fd7e03-0' immediately...
19:02:55.099 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  ...  tolls_amount  improvement_surcharge total_amount  congestion_surcharge
0         1  2019-02-01 00:59:04   2019-02-01 01:07:27                1  ...           0.0                    0.3         12.3                   0.0
1         1  2019-02-01 00:33:09   2019-02-01 01:03:58                1  ...           0.0                    0.3         33.3                   0.0

[2 rows x 18 columns]
19:02:55.102 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                          int64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                   int64
trip_distance                   float64
RatecodeID                        int64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                      int64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
19:02:55.103 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 7019375
19:02:55.141 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
19:02:55.184 | INFO    | Flow run 'energetic-panda' - Created task run 'write_local-f322d1be-0' for task 'write_local'
19:02:55.185 | INFO    | Flow run 'energetic-panda' - Executing 'write_local-f322d1be-0' immediately...
19:03:20.542 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
19:03:20.582 | INFO    | Flow run 'energetic-panda' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
19:03:20.583 | INFO    | Flow run 'energetic-panda' - Executing 'write_gcs-1145c921-0' immediately...
19:03:20.734 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'dtc_data_lake_inner-bonus-375206'.
19:03:20.887 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2019-02.parquet') to the bucket 'dtc_data_lake_inner-bonus-375206' path 'data/yellow/yellow_tripdata_2019-02.parquet'.
19:03:21.584 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
19:03:21.636 | INFO    | Flow run 'energetic-panda' - Finished in state Completed('All states completed.')
19:03:21.763 | INFO    | Flow run 'platinum-armadillo' - Created subflow run 'archetypal-chamois' for flow 'etl-gcs-to-bq'
19:03:21.874 | INFO    | Flow run 'archetypal-chamois' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
19:03:21.876 | INFO    | Flow run 'archetypal-chamois' - Executing 'extract_from_gcs-968e3b65-0' immediately...
19:03:22.136 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Downloading blob named data/yellow/yellow_tripdata_2019-02.parquet from the dtc_data_lake_inner-bonus-375206 bucket to ../data/data/yellow/yellow_tripdata_2019-02.parquet
19:03:25.281 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
19:03:25.327 | INFO    | Flow run 'archetypal-chamois' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
19:03:25.329 | INFO    | Flow run 'archetypal-chamois' - Executing 'write_bq-b366772c-0' immediately...
19:03:58.862 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
19:03:58.918 | INFO    | Flow run 'archetypal-chamois' - Finished in state Completed('All states completed.')
19:03:59.043 | INFO    | Flow run 'platinum-armadillo' - Created subflow run 'cooperative-dinosaur' for flow 'etl-web-to-gcs'
19:03:59.156 | INFO    | Flow run 'cooperative-dinosaur' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
19:03:59.158 | INFO    | Flow run 'cooperative-dinosaur' - Executing 'fetch-b4598a4a-0' immediately...
19:05:01.549 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
19:05:02.467 | INFO    | Flow run 'cooperative-dinosaur' - Created task run 'clean-b9fd7e03-0' for task 'clean'
19:05:02.468 | INFO    | Flow run 'cooperative-dinosaur' - Executing 'clean-b9fd7e03-0' immediately...
19:05:05.799 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  ...  tolls_amount  improvement_surcharge total_amount  congestion_surcharge
0         1  2019-03-01 00:24:41   2019-03-01 00:25:31                1  ...           0.0                    0.3          3.8                   0.0
1         1  2019-03-01 00:25:27   2019-03-01 00:36:37                2  ...           0.0                    0.3         15.0                   0.0

[2 rows x 18 columns]
19:05:05.801 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                          int64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                   int64
trip_distance                   float64
RatecodeID                        int64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                      int64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
19:05:05.803 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 7832545
19:05:05.844 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
19:05:05.885 | INFO    | Flow run 'cooperative-dinosaur' - Created task run 'write_local-f322d1be-0' for task 'write_local'
19:05:05.886 | INFO    | Flow run 'cooperative-dinosaur' - Executing 'write_local-f322d1be-0' immediately...
19:05:34.714 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
19:05:34.761 | INFO    | Flow run 'cooperative-dinosaur' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
19:05:34.762 | INFO    | Flow run 'cooperative-dinosaur' - Executing 'write_gcs-1145c921-0' immediately...
19:05:34.916 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'dtc_data_lake_inner-bonus-375206'.
19:05:35.049 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2019-03.parquet') to the bucket 'dtc_data_lake_inner-bonus-375206' path 'data/yellow/yellow_tripdata_2019-03.parquet'.
19:05:35.759 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
19:05:35.806 | INFO    | Flow run 'cooperative-dinosaur' - Finished in state Completed('All states completed.')
19:05:35.929 | INFO    | Flow run 'platinum-armadillo' - Created subflow run 'delectable-turtle' for flow 'etl-gcs-to-bq'
19:05:36.043 | INFO    | Flow run 'delectable-turtle' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
19:05:36.045 | INFO    | Flow run 'delectable-turtle' - Executing 'extract_from_gcs-968e3b65-0' immediately...
19:05:36.361 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Downloading blob named data/yellow/yellow_tripdata_2019-03.parquet from the dtc_data_lake_inner-bonus-375206 bucket to ../data/data/yellow/yellow_tripdata_2019-03.parquet
19:05:39.077 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
19:05:39.120 | INFO    | Flow run 'delectable-turtle' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
19:05:39.121 | INFO    | Flow run 'delectable-turtle' - Executing 'write_bq-b366772c-0' immediately...
19:06:20.259 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
19:06:20.310 | INFO    | Flow run 'delectable-turtle' - Finished in state Completed('All states completed.')
19:06:20.352 | INFO    | Flow run 'platinum-armadillo' - Finished in state Completed('All states completed.')