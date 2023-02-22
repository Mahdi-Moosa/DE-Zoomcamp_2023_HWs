# Week-4 Homework Solution Steps

* Link to week-4 contents: [DE Zoomcamp 2023 Week-4] (https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering)
* Link to the Homeworks for week-4: [link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_4_analytics_engineering/homework.md)
* Link to the DBT project repository: [link](https://github.com/Mahdi-Moosa/dbt_de_zoomcamp-2023-week-4)

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442
- 51648442
- 61648442
- 71648442

#### Solution

* Step 1: Ingest data. Data ingestion scripts: [*from-web-to-gcs*](https://github.com/Mahdi-Moosa/DE-Zoomcamp_2023_HWs/blob/main/Week_4_Homeworks/from_web_to_gcs_parquet.py) and [*from-gcs-to-bq*](https://github.com/Mahdi-Moosa/DE-Zoomcamp_2023_HWs/blob/main/Week_4_Homeworks/el_gcs_to_bq.py)
* Step 2: Perfrom data transformation using DBT (link to repo: https://github.com/Mahdi-Moosa/dbt_de_zoomcamp-2023-week-4)
* Step 3: Query database:

      SELECT
      COUNT(*)
      FROM
      `de-zoomcamp-mmm.dbt_mmm.fact_trips`
      WHERE
      EXTRACT(year
      FROM
          pickup_datetime) = 2019
      OR EXTRACT(year
      FROM
          pickup_datetime) = 2020;

* Step 4: Recorded query output: 61597007

**ANS**: Closest answer available: 61648442

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

#### Solution

*Google Data Studio Snapshot on pie chart (service type-filtering).*

![service type filter fhv data](https://user-images.githubusercontent.com/82473321/220749701-7790719b-851f-4bfb-8707-84206b63552f.JPG)

Recorded answer: 89.8/10.2

**ANS**: Closest available answer: 89.9/10.1

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696
- 53244696
- 63244696

#### Solution

* Step 1-2: Similar to Q1 ans.
* Step 3: Run query:

          SELECT
          COUNT(*)
          FROM
          `dbt_mmm.stg_fhv_tripdata`
          WHERE
          EXTRACT(year
          FROM
              pickup_datetime)=2019;

* Step 4: Recorded answer: 43244696

**ANS**: 43244696

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722
- 32998722
- 42998722

#### Solution
Run query:

    SELECT
    COUNT(*)
    FROM
    `dbt_mmm.fact_fhv_trips`
    WHERE
    EXTRACT(year
    FROM
        pickup_datetime)=2019;

Recorded answer: 22998722

**ANS**: 22998722

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December

#### Solution

*Google Data Studio Snapshot on month-aggregated-time-series.*

![Month-aggregated-time-series](https://user-images.githubusercontent.com/82473321/220749132-9c936c2b-820d-495d-a064-f1375330d755.jpg)

Record answer: March 2019

**ANS**: March
