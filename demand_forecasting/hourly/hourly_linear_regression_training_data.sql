WITH SimulAgg AS (
	SELECT 
		city_destination, 
		delivery_timestamp, 
       	day_of_week(delivery_timestamp) AS delivery_timestamp_dow,
        hour(delivery_timestamp) AS delivery_timestamp_h,
		count(DISTINCT ride_uuid) AS total_demand
	FROM hive.bi_vlt.vlt_demand_prediction_simul_rides
	WHERE 
		rides_demanded = 1 AND 
		delivery_timestamp < current_date 
	GROUP BY 1,2,3,4
),
RA AS (
	SELECT 
		*,
	    AVG(total_demand) OVER (
	        PARTITION BY delivery_timestamp_dow, delivery_timestamp_h
	        ORDER BY delivery_timestamp
	        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
	    ) AS rolling_dh_avg2,
	    AVG(total_demand) OVER (
	        PARTITION BY delivery_timestamp_dow, delivery_timestamp_h
	        ORDER BY delivery_timestamp
	        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
	    ) AS rolling_dh_avg4,
	    AVG(total_demand) OVER (
	        PARTITION BY delivery_timestamp_dow
	        ORDER BY delivery_timestamp
	        ROWS BETWEEN 168 PRECEDING AND CURRENT ROW
	    ) AS rolling_d_avg7,
	    AVG(total_demand) OVER (
	        PARTITION BY delivery_timestamp_dow
	        ORDER BY delivery_timestamp
	        ROWS BETWEEN 336 PRECEDING AND CURRENT ROW
	    ) AS rolling_d_avg14
	FROM 
		SimulAgg
), 
DateSeq AS (
	SELECT t.observation_date FROM UNNEST(SEQUENCE(current_date - INTERVAL '12' MONTH, current_date,INTERVAL '1' DAY)) AS t(observation_date)
), 
SeqNum14 AS (
	SELECT 
		*,
	    ROW_NUMBER() OVER (partition by city_destination, observation_date ORDER BY delivery_timestamp)-1 AS seqnum 
	FROM 
		RA
	LEFT JOIN DateSeq ds ON 
		DATE(delivery_timestamp) BETWEEN observation_date - INTERVAL '14' DAY AND observation_date
	ORDER BY observation_date, delivery_timestamp
), 
ExpMovingAvg AS (
	SELECT 
		city_destination, 
		observation_date,
		delivery_timestamp, 
		date_diff('day', date(delivery_timestamp),observation_date) AS date_diff, 
		delivery_timestamp_dow,
		delivery_timestamp_h,
		total_demand,
		-----
	    ROUND((SUM(POWER((1 / 0.4), seqnum) * rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.4), seqnum + 1)) AS rolling_dh_avg4_exp_mov_avg4,
	    ROUND((SUM(POWER((1 / 0.5), seqnum) * rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.5), seqnum + 1)) AS rolling_dh_avg4_exp_mov_avg5,
	    ROUND((SUM(POWER((1 / 0.6), seqnum) * rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg4) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.6), seqnum + 1)) AS rolling_dh_avg4_exp_mov_avg6,
		-----
	    ROUND((SUM(POWER((1 / 0.4), seqnum) * rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.4), seqnum + 1)) AS rolling_dh_avg2_exp_mov_avg4,
	    ROUND((SUM(POWER((1 / 0.5), seqnum) * rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.5), seqnum + 1)) AS rolling_dh_avg2_exp_mov_avg5,
	    ROUND((SUM(POWER((1 / 0.6), seqnum) * rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_dh_avg2) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.6), seqnum + 1)) AS rolling_dh_avg2_exp_mov_avg6,
	    -----
	    ROUND((SUM(POWER((1 / 0.4), seqnum) * rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.4), seqnum + 1)) AS rolling_d_avg7_exp_mov_avg4,
	    ROUND((SUM(POWER((1 / 0.5), seqnum) * rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.5), seqnum + 1)) AS rolling_d_avg7_exp_mov_avg5,
	    ROUND((SUM(POWER((1 / 0.6), seqnum) * rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg7) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.6), seqnum + 1)) AS rolling_d_avg7_exp_mov_avg6,
	    -----
	    ROUND((SUM(POWER((1 / 0.4), seqnum) * rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.4), seqnum + 1)) AS rolling_d_avg14_exp_mov_avg4,
	    ROUND((SUM(POWER((1 / 0.5), seqnum) * rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.5), seqnum + 1)) AS rolling_d_avg14_exp_mov_avg5,
	    ROUND((SUM(POWER((1 / 0.6), seqnum) * rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
	        FIRST_VALUE(rolling_d_avg14) OVER (partition by city_destination, observation_date ORDER BY seqnum)
	    ) / POWER((1 / 0.6), seqnum + 1)) AS rolling_d_avg14_exp_mov_avg6
	FROM 
		SeqNum14
	WHERE 
		observation_date > date('2024-01-01') 
), 
AvgExpMovingAvg AS (
	SELECT 
		city_destination, 
		observation_date, 
		delivery_timestamp_dow,
		delivery_timestamp_h,
		AVG(rolling_dh_avg4_exp_mov_avg4) AS avg_rolling_dh_avg4_exp_mov_avg4,
		AVG(rolling_dh_avg4_exp_mov_avg5) AS avg_rolling_dh_avg4_exp_mov_avg5,
	    AVG(rolling_dh_avg4_exp_mov_avg6) AS avg_rolling_dh_avg4_exp_mov_avg6,
		AVG(rolling_dh_avg2_exp_mov_avg4) AS avg_rolling_dh_avg2_exp_mov_avg4,
		AVG(rolling_dh_avg2_exp_mov_avg5) AS avg_rolling_dh_avg2_exp_mov_avg5,
	    AVG(rolling_dh_avg2_exp_mov_avg6) AS avg_rolling_dh_avg2_exp_mov_avg6,
	    AVG(rolling_d_avg14_exp_mov_avg4) AS avg_rolling_d_avg14_exp_mov_avg4,
	    AVG(rolling_d_avg14_exp_mov_avg5) AS avg_rolling_d_avg14_exp_mov_avg5,
	    AVG(rolling_d_avg14_exp_mov_avg6) AS avg_rolling_d_avg14_exp_mov_avg6,
	    AVG(rolling_d_avg7_exp_mov_avg4) AS avg_rolling_d_avg7_exp_mov_avg4,
	    AVG(rolling_d_avg7_exp_mov_avg5) AS avg_rolling_d_avg7_exp_mov_avg5,
	    AVG(rolling_d_avg7_exp_mov_avg6) AS avg_rolling_d_avg7_exp_mov_avg6
	FROM ExpMovingAvg
	GROUP BY 1,2,3,4
),
CancelationLeadTime AS (
	SELECT 
		city_destination, 
		delivery_timestamp,
		least(28, date_diff('day', canceled_at, booked_start_at)) AS cancelation_lead_time, 
		count(DISTINCT ride_uuid) AS total_cancelations
	FROM 
		hive.bi_vlt.vlt_demand_prediction_simul_rides
	WHERE
		state LIKE '%Cancel%' AND 
		NOT is_emirates AND 
		YEAR(booked_start_at) > 2021 
	GROUP BY 1,2,3
), 
CancelationLeadTimeRS AS (
	SELECT 
		*,
		SUM(total_cancelations) OVER (PARTITION BY city_destination, delivery_timestamp ORDER BY cancelation_lead_time DESC) AS total_cancelations_rs
	FROM 
		CancelationLeadTime 
),
CancelationLeadTimeRSLAG AS (
	SELECT 
		*, 
		COALESCE(LAG(cancelation_lead_time) OVER (PARTITION BY city_destination, delivery_timestamp ORDER BY cancelation_lead_time),-1) AS next_cancelation_lead_time
	FROM CancelationLeadTimeRS 
), 
Finale AS (
	SELECT	
		f.city_destination || CAST(booked_start_at_local AS VARCHAR) || CAST(lead_time AS VARCHAR) AS forecast_id,
		f.city_destination, 
		booked_start_at_local, 
		DATE(booked_start_at_local) - INTERVAL '1' DAY * lead_time AS observation_date, 
		CAST(day_of_week(booked_start_at_local) > 5 AS INT) AS delivery_date_weekday,
		day_of_week(booked_start_at_local) AS delivery_date_dow, 
		hour(booked_start_at_local) AS delivery_date_h,
		f.lead_time,
		total_booked_rs, 
		perc_booked_running_sum,
		avg_rolling_dh_avg4_exp_mov_avg4,
		avg_rolling_dh_avg4_exp_mov_avg5,
	    avg_rolling_dh_avg4_exp_mov_avg6,
		avg_rolling_dh_avg2_exp_mov_avg4,
		avg_rolling_dh_avg2_exp_mov_avg5,
		avg_rolling_dh_avg2_exp_mov_avg6,
		avg_rolling_d_avg14_exp_mov_avg4,
		avg_rolling_d_avg14_exp_mov_avg5,
		avg_rolling_d_avg14_exp_mov_avg6,
		avg_rolling_d_avg7_exp_mov_avg4,
		avg_rolling_d_avg7_exp_mov_avg5,
		avg_rolling_d_avg7_exp_mov_avg6,
		total_cancelations_rs,
		total_h_demand AS target, 
		forecast_demand AS old_forecast
	FROM 
		hive.bi_vlt.vlt_demand_prediction_simul_rides_past_forecasts f 
	LEFT JOIN AvgExpMovingAvg aema ON 
		aema.city_destination = f.city_destination AND
		aema.delivery_timestamp_dow = day_of_week(booked_start_at_local) AND
		aema.delivery_timestamp_h = hour(booked_start_at_local) AND 
		aema.observation_date = DATE(booked_start_at_local) - INTERVAL '1' DAY * lead_time 
	LEFT JOIN CancelationLeadTimeRSLAG ctr ON 
		ctr.city_destination = f.city_destination AND 
		ctr.delivery_timestamp = f.booked_start_at_local AND 
		ctr.cancelation_lead_time >= f.lead_time AND 
		ctr.next_cancelation_lead_time < f.lead_time  	
	WHERE 
		booked_start_at_local > date('2024-01-01') 
)
SELECT 
	*
FROM 	
	Finale;
