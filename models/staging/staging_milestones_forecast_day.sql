WITH forecast_day_surfing AS (
    SELECT 
            -- general variables
            (extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date')::DATE AS date
            ,(extracted_data ->'location'->>'name')::VARCHAR(255) AS city
            ,(extracted_data -> 'location' ->> 'region')::VARCHAR(255) AS region
            ,(extracted_data -> 'location' ->> 'country')::VARCHAR(255) AS country
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'maxtemp_c')::NUMERIC AS max_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'mintemp_c')::NUMERIC AS min_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgtemp_c')::NUMERIC AS avg_temp_c
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'uv')::NUMERIC AS uv
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'sunrise') AS sunrise
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' ->> 'sunset') AS sunset
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'totalprecip_mm')::NUMERIC AS total_precip_mm
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avgvis_km')::NUMERIC AS avg_vis_km
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'avghumidity')::NUMERIC AS avg_humidity
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_will_it_rain')::INT AS daily_will_it_rain
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'daily_chance_of_rain')::INT AS daily_chance_of_rain
            
            -- surfing specific (wind and marine)
            -- wind
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' ->> 'maxwind_kph')::NUMERIC AS max_wind_kph

            -- Marine Weather -> Tides ALL NULL VALUES
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'tides' ->> 'tide_time')::NUMERIC AS tide_time
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'tides' ->> 'tide_height_mt')::NUMERIC AS tide_height_mt
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'tides' ->> 'tide_type')::NUMERIC AS tide_type               
    
            -- Marine Weather -> Hour ALL NULL VALUES
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'Hour' ->> 'water_temp_c')::NUMERIC AS water_temp_c 
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'Hour' ->> 'sig_ht_mt')::NUMERIC AS sig_ht_mt
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'Hour' ->> 'swell_ht_mt')::NUMERIC AS swell_ht_mt
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'Hour' ->> 'swell_dir')::NUMERIC AS swell_dir    
            --,(extracted_data -> 'forecast' -> 'Marine weather' -> 0 -> 'Hour' ->> 'swell_period_secs')::NUMERIC AS swell_period_secs   
 

            -- condition information (text, icon, code)
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'text')::VARCHAR(255) AS condition_text
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'icon')::VARCHAR(255) AS condition_icon
            ,(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' ->> 'code')::VARCHAR(255) AS condition_code


    FROM {{source("staging", "weather_raw")}})
SELECT * 
FROM forecast_day_surfing