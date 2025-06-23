
-- 1. Top 5 Highest-Revenue Pickup Locations

SELECT 
  pickup_location,
  location_name,
  city,
  state,
  SUM(total_revenue) AS total_revenue
FROM vlm_vehicle_location_metrics
GROUP BY pickup_location, location_name, city, state
ORDER BY total_revenue DESC
LIMIT 5;


--  2. Most Rented Vehicle Type per Location
WITH RankedMetrics AS (
  SELECT
    pickup_location,
    location_name,
    vehicle_type,
    total_transactions,
    ROW_NUMBER() OVER (PARTITION BY pickup_location ORDER BY total_transactions DESC) as rn
  FROM vlm_vehicle_location_metrics
)
SELECT
  pickup_location,
  location_name,
  vehicle_type,
  total_transactions
FROM RankedMetrics
WHERE rn = 1;

