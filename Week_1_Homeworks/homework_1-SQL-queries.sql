select count(index) from green_taxi_trips where date(lpep_pickup_datetime)='2019-01-15' and date (lpep_dropoff_datetime) = '2019-01-15';

select trip_distance, lpep_pickup_datetime from green_taxi_trips order by trip_distance desc limit 5;

select count(index) from green_taxi_trips where date(lpep_pickup_datetime)='2019-01-01' and passenger_count = 2;

select count(index) from green_taxi_trips where date(lpep_pickup_datetime)='2019-01-01' and passenger_count = 3;

SELECT l."Zone" as pickup_zone, m.tip_amount, r."Zone" as dropoff_zone
 FROM zones l, green_taxi_trips m, zones r
 WHERE l."LocationID" = m."PULocationID" AND  m."DOLocationID" = r."LocationID" AND l."Zone"='Astoria'
 ORDER BY tip_amount DESC
 LIMIT 5;