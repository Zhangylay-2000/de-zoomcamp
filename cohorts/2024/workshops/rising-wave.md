## Stream processing with Rising Wave

#Q1   

CREATE MATERIALIZED VIEW avg_max_min AS
	select
		taxi_zone."Zone" as zone1,
		taxi_zone2."Zone" as zone2,
		COUNT(*) as n_trips,
		AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as avg_d,
		MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as max_d,
		MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as min_d
	FROM 
		public.trip_data
	left JOIN 
		public.taxi_zone 
	ON 
		trip_data."PULocationID" = taxi_zone."LocationID"
	left JOIN 
		public.taxi_zone as taxi_zone2 
	ON 
		trip_data."DOLocationID" = taxi_zone2."LocationID"
	group by
		zone1,
		zone2
	order by
		avg_d DESC   

![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/ed263c3a-a0e7-40e0-b74e-f4107184e0d1)

![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/3ade1fcb-e3f9-418e-86df-8b8dc4451422)

		
#Q2
select * from avg_max_min
order by avg_d desc;

![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/20b9a6e9-4c35-44e5-9f64-1e40559dc3a5)

![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/688cd970-4817-4612-9852-6c6d99502678)


#Q3
CREATE MATERIALIZED VIEW latest_pickup_time AS
    WITH t AS (
        SELECT MAX(trip_data.tpep_pickup_datetime) AS latest_pickup_time
        FROM public.trip_data
    )
    SELECT 
    	taxi_zone."Zone" as taxi_zone,
    	COUNT(*) as cnt
    FROM t,
            public.trip_data
    JOIN public.taxi_zone 
        ON trip_data."PULocationID" = taxi_zone."LocationID"
    WHERE trip_data.tpep_pickup_datetime BETWEEN t.latest_pickup_time - interval '17' hour and t.latest_pickup_time
   	group by 
   		taxi_zone 
   	order by
   		cnt DESC

![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/339cb7b1-81e5-486b-a213-fbc55e42090e)

     
![image](https://github.com/Zhangylay-2000/de-zoomcamp/assets/68446698/a1733ab4-f525-485c-9eff-af0d02b40182)




