WITH forecast_day_data AS (
    SELECT * 
    FROM {{ref('staging_milestones_forecast_day')}}
),
add_features AS (
    SELECT *
        ,date_part('day', date) AS day_of_month           -- day of month as a number
        ,to_char(date, 'Month') AS month_of_year          -- month name as a text
        ,date_part('year', date) AS year                  -- year as a number
        ,to_char(date, 'Day') AS day_of_week              -- weekday name as text
        ,date_part('week', date) AS week_of_year          -- calender week number as number
        ,to_char(date, 'IYYY-IW') AS year_and_week        -- year-calenderweek as text like '2024-43'

    FROM forecast_day_data
)
SELECT *
FROM add_features
