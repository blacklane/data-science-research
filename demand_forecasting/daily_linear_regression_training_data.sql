WITH TotalRides AS (
	SELECT 
		city_destination, 
		date(booked_start_at_local) AS ride_date, 
		count(DISTINCT ride_uuid) AS total_rides
	FROM 
		hive.bi_vlt.vlt_self_service_facts 
	WHERE 	
		city_destination = 'Barcelona'  AND 
		rides_demanded = 1 AND 
		YEAR(booked_start_at_local) > 2022
	GROUP BY 1,2
), 
MovingAvg AS (
	SELECT 
		city_destination, 
		ride_date, 
		total_rides,
	    AVG(total_rides) OVER (ORDER BY city_destination, ride_date ROWS BETWEEN 2 PRECEDING AND 0 FOLLOWING) AS "smoothen_average_pred_3",
	    AVG(total_rides) OVER (ORDER BY city_destination, ride_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS "smoothen_average_pred_7",
		AVG(total_rides) OVER (ORDER BY city_destination, ride_date ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS "smoothen_average_pred_14",
		AVG(total_rides) OVER (ORDER BY city_destination, ride_date ROWS BETWEEN 20 PRECEDING AND 0 FOLLOWING) AS "smoothen_average_pred_21"
	FROM 
		TotalRides
),
PastForecasts AS (
	SELECT
        *,
  		ROW_NUMBER() OVER (partition by city_destination, p.observation_date ORDER BY p.delivery_date )-1 AS seqnum
    FROM bi_vlt.vlt_demand_prediction_past_forecasts p
    WHERE city_destination = 'Barcelona'
), 
ExpMovingAvg AS (
	SELECT
	    p.city_destination,
	    p.observation_date,
	    p.delivery_date, 
	    p.total_booked_cancelation_prediction,
	    ROUND((SUM(POWER((1 / 0.4), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.4), seqnum + 1)) AS exp_mov_avg4,
	    ROUND((SUM(POWER((1 / 0.5), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.5), seqnum + 1)) AS exp_mov_avg5,
	    ROUND((SUM(POWER((1 / 0.6), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.6), seqnum + 1)) AS exp_mov_avg6
	FROM 
		PastForecasts p 
)
SELECT 
	pf.city_destination, 
	pf.observation_date, 
	pf.delivery_date, 
	pf.lead_time,
	exp_mov_avg4,
	exp_mov_avg5,
	exp_mov_avg6,
	"smoothen_average_pred_3",
	"smoothen_average_pred_7",
	"smoothen_average_pred_14",
	"smoothen_average_pred_21", 
	pf.total_booked_cancelation_prediction,
	pf.target_demanded
FROM 
	PastForecasts pf
LEFT JOIN MovingAvg ma ON 
	ma.city_destination = pf.city_destination AND 
	ma.ride_date - INTERVAL '1' DAY = pf.observation_date 
LEFT JOIN ExpMovingAvg ema ON 
	ema.city_destination = pf.city_destination AND 
	ema.observation_date = pf.observation_date AND 
	ema.delivery_date = pf.delivery_date 
WHERE YEAR(pf.observation_date) > 2022 
ORDER BY 2,3