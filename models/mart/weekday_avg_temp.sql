WITH weekday_avg_temp AS (
    SELECT * 
    FROM {{ref('prep_forecast_day')}}
),
add_features AS (
    SELECT *
        ,to_char(date, 'Day') AS day_of_week 
        --,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgtemp_c')::NUMERIC AS avg_temp_c

    FROM weekday_avg_temp
)
SELECT *
FROM add_features