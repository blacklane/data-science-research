SELECT
    p.city_destination,
    p.ride_date,
    p.total_bookings,
    p.total_booked_cancelation_prediction,
    ROUND((SUM(POWER((1 / 0.5), seqnum) * total_booked_cancelation_prediction) OVER (partition by city_destination ORDER BY seqnum) +
        FIRST_VALUE(total_booked_cancelation_prediction) OVER (partition by city_destination ORDER BY seqnum)
    ) / POWER((1 / 0.5), seqnum + 1)) AS exp_mov_avg
FROM (
    SELECT
        p.city_destination, p.ride_date, p.total_bookings, p.total_booked_cancelation_prediction,
        ROW_NUMBER() OVER (partition by city_destination ORDER BY p.city_destination, p.ride_date )-1 AS seqnum
    FROM
        bi_vlt.vlt_demand_prediction_forecast p
) p;
