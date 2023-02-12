-- Q1
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv`
OPTIONS (
  format = 'csv',
  uris = ['gs://mmm-de-zoomcamp/data/fhv/*.csv.gz']
);

SELECT COUNT(*)
FROM `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv`;

-- Q2
  SELECT
    DISTINCT COUNT(Affiliated_base_number)
  FROM
    `dezoomcamp_ny_taxi.fhv_internal`;

-- Q3
SELECT 
    COUNT(*) 
FROM `dezoomcamp_ny_taxi.fhv_internal` 
WHERE 
    PUlocationID IS NULL 
    AND DOlocationID IS NULL;

-- Q5
CREATE OR REPLACE TABLE
  `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv_internal_partitioned`
PARTITION BY
  DATE(pickup_datetime) AS
SELECT
  *
FROM
  `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv_internal`;

SELECT
  DISTINCT affiliated_base_number
FROM
  `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv_internal`
WHERE
  pickup_datetime BETWEEN '2019-03-01'
  AND '2019-03-31';

SELECT
  DISTINCT affiliated_base_number
FROM
  `de-zoomcamp-mmm.dezoomcamp_ny_taxi.fhv_internal_partitioned`
WHERE
  pickup_datetime BETWEEN '2019-03-01'
  AND '2019-03-31';