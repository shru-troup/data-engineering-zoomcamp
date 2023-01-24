# Homework 1

**Q3: How many taxi trips were totally made on January 15? 20530**

```sql
SELECT COUNT(*) FROM green_taxi_trips
WHERE
    CAST(lpep_pickup_datetime as DATE) = '2019-01-15' AND
    CAST(lpep_dropoff_datetime as DATE) = '2019-01-15'
```

**Q4: Which was the day with the largest trip distance? 2019-01-15**

```sql
SELECT * FROM green_taxi_trips
WHERE
    trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips)
```

**Q5: In 2019-01-01 how many trips had 2 and 3 passengers? 2: 1282, 3: 254**

```sql
SELECT COUNT(*) FROM green_taxi_trips
WHERE
    CAST(lpep_pickup_datetime as DATE) = '2019-01-01' AND passenger_count = 2
```
**Q6. For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?  Long Island City/Queens Plaza**

```sql
SELECT t.tip_amount, zd."Zone"
FROM green_taxi_trips t 
    JOIN zones zu ON t."PULocationID" = zu."LocationID"
    JOIN zones zd ON t."DOLocationID" = zd."LocationID"
WHERE 
    zu."Zone" = 'Astoria'
ORDER BY tip_amount DESC
LIMIT 1;
```