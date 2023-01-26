SELECT count(*)
FROM yellow_taxi_data ytd
WHERE lpep_pickup_datetime >= '2019-01-15'
  AND lpep_pickup_datetime < '2019-01-16';


SELECT *
FROM yellow_taxi_data ytd
ORDER BY trip_distance DESC
LIMIT 1;


SELECT count(*),
       passenger_count
FROM yellow_taxi_data ytd
WHERE lpep_pickup_datetime >= '2019-01-01'
  AND lpep_pickup_datetime < '2019-01-02'
GROUP BY passenger_count;


SELECT z2."Zone",
       tip_amount
FROM
  (SELECT *
   FROM yellow_taxi_data ytd
   JOIN zones z ON z."LocationID" = ytd."PULocationID"
   WHERE z."Zone" = 'Astoria'
   ORDER BY tip_amount DESC
   LIMIT 1) AS max_tip
JOIN zones z2 ON max_tip."DOLocationID" = z2."LocationID";