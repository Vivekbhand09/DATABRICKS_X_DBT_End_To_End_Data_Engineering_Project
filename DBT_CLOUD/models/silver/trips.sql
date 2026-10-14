
{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

{% set cols = [
    'trip_id',
    'customer_id',
    'vehicle_id',
    'trip_start_time',
    'driver_id',
    'trip_end_time',
    'distance_km',
    'fare_amount',
    'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM {{ source('source_bronze', 'trips') }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(
        MAX(last_updated_timestamp),
        CAST('1900-01-01' AS TIMESTAMP)
    )
    FROM {{ this }}
)
{% endif %}
