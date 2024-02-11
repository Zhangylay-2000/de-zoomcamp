```ruby 
--q1; --840402
select count(lpep_pickup_datetime)```
 ```from de-zoomcamp-411813.ny_taxi.external_green_tripdata_non_partitoned;```

--q2; --258
--2.14 MB for the External Table and 0MB for the Materialized Table
```select count(distinct PULocationID) from de-zoomcamp-411813.ny_taxi.external_green_tripdata_non_partitoned;```

--q3; --1622
```select count(lpep_pickup_datetime) from de-zoomcamp-411813.ny_taxi.external_green_tripdata_non_partitoned where  fare_amount=0;```

--q4;
--- Partition by lpep_pickup_datetime Cluster on PUlocationID 
```CREATE OR REPLACE TABLE de-zoomcamp-411813.ny_taxi.external_green_tripdata_par_cl PARTITION BY DATE(lpep_pickup_datetime) CLUSTER BY PUlocationID AS SELECT * FROM de-zoomcamp-411813.ny_taxi.external_green_tripdata_non_partitoned;```
```select * from de-zoomcamp-411813.ny_taxi.external_green_tripdata_par_cl limit 100;```

--q5;
```SELECT DISTINCT(PULocationID) FROM de-zoomcamp-411813.ny_taxi.external_green_tripdata_non_partitoned WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';```
--12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

--q6;
--Where is the data stored in the External Table you created?
--GCP Bucket

--q7
--It is best practice in Big Query to always cluster your data:
--False














