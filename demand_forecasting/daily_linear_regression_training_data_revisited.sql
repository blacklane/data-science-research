WITH TotalRides AS (
	SELECT
		city_destination,
		date(booked_start_at_local) AS ride_date,
		count(DISTINCT ride_uuid) AS total_rides
	FROM
		hive.bi_vlt.vlt_self_service_facts
	WHERE
		--city_destination = 'Barcelona'  AND
		rides_demanded = 1 AND
		YEAR(booked_start_at_local) > 2022 AND
		NOT is_emirates
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
    --WHERE city_destination = 'Barcelona'
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
),
CancelationLeadTime AS (
	SELECT
		city_destination,
		date(booked_start_at_local) AS delivery_date,
		least(28, date_diff('day', canceled_at, booked_start_at)) AS cancelation_lead_time,
		count(DISTINCT ride_uuid) AS total_cancelations
	FROM
		hive.bi_vlt.vlt_self_service_facts
	WHERE
		state LIKE '%Cancel%' AND
		NOT is_emirates AND
		YEAR(booked_start_at) > 2021 /*AND
		city_destination = 'Barcelona'*/
	GROUP BY 1,2,3
),
CancelationLeadTimeRS AS (
	SELECT
		*,
		delivery_date  - INTERVAL '1' DAY * cancelation_lead_time AS observation_date,
		SUM(total_cancelations) OVER (PARTITION BY city_destination, delivery_date ORDER BY cancelation_lead_time DESC) AS total_cancelations_rs
	FROM
		CancelationLeadTime
),
CancelationLeadTimeRSLAG AS (
	SELECT
		*,
		COALESCE(LAG(cancelation_lead_time) OVER (PARTITION BY city_destination, delivery_date ORDER BY cancelation_lead_time),-1) AS next_cancelation_lead_time
	FROM
		CancelationLeadTimeRS
),
CorpGrouping AS (
	SELECT
		city_destination,
		date(booked_start_at_local) AS ride_date,
		least(28,date_diff('day',created_at,booked_start_at)) AS lead_time,
		COALESCE(corporation_name, 'b2c') AS booker,
		count(DISTINCT ride_uuid) AS total_bookings
	FROM
		hive.bi_vlt.vlt_self_service_facts
	WHERE
		booked_start_at_local < current_date AND
		NOT is_emirates/*AND
		city_destination = 'Barcelona'*/
	GROUP BY
		1,2,3,4
),
CorpGroupingDistinct AS (
	SELECT
		DISTINCT cg.city_destination,
		cg.ride_date,
		lt.lead_time,
		cg.booker,
		COALESCE(cg2.total_bookings,0) AS total_bookings
	FROM CorpGrouping cg
	CROSS JOIN UNNEST(SEQUENCE(0,28,1)) AS lt(lead_time)
	LEFT JOIN CorpGrouping  cg2 ON
		lt.lead_time = cg2.lead_time AND
		cg2.city_destination = cg.city_destination AND
		cg2.ride_date = cg.ride_date AND
		cg2.booker = cg.booker
),
CorpGroupingRS AS (
	SELECT
		*,
		SUM(total_bookings) OVER (PARTITION BY city_destination, booker, ride_date ORDER BY lead_time DESC) AS total_bookings_rs
	FROM
		CorpGroupingDistinct
),
CorpGroupingRN AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY city_destination, ride_date, lead_time ORDER BY total_bookings_rs DESC) AS rn
	FROM
		CorpGroupingRS
)
SELECT
	pf.city_destination,
	pf.observation_date,
	year(pf.observation_date) AS observation_date_y,
	week(pf.observation_date) AS observation_date_w,
	day_of_week(pf.observation_date) AS observation_date_d,
	pf.delivery_date,
	year(pf.delivery_date) AS delivery_date_y,
	week(pf.delivery_date) AS delivery_date_w,
	day_of_week(pf.delivery_date) AS delivery_date_d,
	pf.lead_time,
	exp_mov_avg4,
	exp_mov_avg5,
	exp_mov_avg6,
	/*"smoothen_average_pred_3",
	"smoothen_average_pred_7",
	"smoothen_average_pred_14",
	"smoothen_average_pred_21", */
	pf.total_booked_cancelation_prediction,
	log10(cast(pf.total_booked_cancelation_prediction AS double)) AS total_booked_cancelation_prediction_log,
	log10(COALESCE(ctr.total_cancelations_rs,0)) AS total_cancelations_rs_log,
	log10(COALESCE(cgr.total_bookings_rs,0)) AS total_bookings_rs_log,
	pf.running_sum_bookings,
	--cancellation_rate,
	--perc_booked_running_sum,
	COALESCE(ctr.total_cancelations_rs,0) AS total_cancelations_rs,
	cgr.total_bookings_rs AS top_booker_amt,
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
LEFT JOIN CancelationLeadTimeRSLAG ctr ON
	ctr.city_destination = pf.city_destination AND
	ctr.delivery_date = pf.delivery_date AND
	ctr.cancelation_lead_time - 1 >= pf.lead_time AND
	ctr.next_cancelation_lead_time -1 < pf.lead_time
LEFT JOIN CorpGroupingRN cgr ON
	cgr.city_destination = pf.city_destination AND
	cgr.ride_date = pf.delivery_date AND
	cgr.lead_time - 1 = pf.lead_time AND
	rn = 1
WHERE pf.observation_date > DATE('2023-11-24')