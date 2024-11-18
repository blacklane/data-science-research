WITH cte AS (

SELECT city_destination,
       observation_date,
       ride_date, total_bookings,
       total_booked_cancelation_prediction
  FROM bi_vlt.vlt_demand_prediction_forecast)

SELECT cte.city_destination as "city",
       cte.ride_date as "day",
       cte.total_bookings as "total_booking",
   	   cte.total_booked_cancelation_prediction as "pred_rides",
       AVG(cte.total_booked_cancelation_prediction) OVER (ORDER BY cte.ride_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS "smoothen_average_pred_7"

  FROM cte
  
 ORDER BY 1,2 DESC;