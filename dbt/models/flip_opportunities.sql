WITH source_data AS (

    SELECT s.item_id, 
            d.name AS item_name, 
            s.window_timestamp_utc AS time_window, 
            s.avg_high_price, 
            s.avg_low_price, 
            s.high_price_volume, 
            s.low_price_volume, (s.avg_high_price * 0.98) - s.avg_low_price AS profit_per_item, 
            d.buy_limit, 
            s.is_complete 
    FROM {{ source('silver', 'silver_prices') }} s 
    LEFT JOIN {{ source('silver', 'dim_items') }} d 
        ON s.item_id = d.item_id

)

SELECT item_id, 
        item_name, 
        time_window, 
        avg_high_price, 
        avg_low_price, 
        high_price_volume, 
        low_price_volume, 
        profit_per_item, 
        ROUND((profit_per_item / avg_low_price) * 100, 2) AS profit_per_item_pct,
        buy_limit, 
        profit_per_item * buy_limit AS total_profit_per_cycle,
        is_complete
FROM source_data