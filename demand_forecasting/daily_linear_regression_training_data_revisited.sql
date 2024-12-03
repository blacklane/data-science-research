WITH PastForecasts AS (
    SELECT
        *,
       ROW_NUMBER() OVER (partition by city_destination, p.observation_date ORDER BY p.delivery_date )-1 AS seqnum
    FROM bi_vlt.vlt_demand_prediction_past_forecasts p
),
ExpMovingAvg AS (
    SELECT
        p.city_destination,
        p.observation_date,
        p.delivery_date,
        p.total_booked_cancelation_prediction,
        ROUND((SUM(POWER((1 / 0.3), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
            FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
        ) / POWER((1 / 0.3), seqnum + 1)) AS exp_mov_avg3,
        ROUND((SUM(POWER((1 / 0.4), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
            FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
        ) / POWER((1 / 0.4), seqnum + 1)) AS exp_mov_avg4,
        ROUND((SUM(POWER((1 / 0.5), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
            FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
        ) / POWER((1 / 0.5), seqnum + 1)) AS exp_mov_avg5,
        ROUND((SUM(POWER((1 / 0.6), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
            FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
        ) / POWER((1 / 0.6), seqnum + 1)) AS exp_mov_avg6,
        ROUND((SUM(POWER((1 / 0.7), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum) +
            FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination, observation_date ORDER BY seqnum)
        ) / POWER((1 / 0.7), seqnum + 1)) AS exp_mov_avg7
    FROM
       PastForecasts p
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
    exp_mov_avg3,
    exp_mov_avg4,
    exp_mov_avg5,
    exp_mov_avg6,
    exp_mov_avg7,
    pf.total_booked_cancelation_prediction,
    pf.target_demanded
FROM
    PastForecasts pf
LEFT JOIN ExpMovingAvg ema ON
    ema.city_destination = pf.city_destination AND
    ema.observation_date = pf.observation_date AND
    ema.delivery_date = pf.delivery_date
WHERE pf.observation_date > DATE('2023-11-24')