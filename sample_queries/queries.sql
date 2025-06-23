
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

--  3. Top 5 Most Active Users (by number of transactions)
SELECT 
  user_id,
  first_name,
  last_name,
  total_transactions,
  total_rental_hours,
  total_spent
FROM user_user_metrics
ORDER BY total_transactions DESC
LIMIT 5;


-- 4. Top 5 Users by Total Spending
SELECT 
  user_id,
  first_name,
  last_name,
  total_spent,
  total_transactions,
  total_rental_hours
FROM user_user_metrics
ORDER BY total_spent DESC
LIMIT 5;

-- 5. Daily Rental Transaction & Revenue Trends
SELECT 
  rental_date,
  daily_transaction_count,
  daily_revenue
FROM daily_daily_metrics
ORDER BY rental_date;

-- 6. Day with the Highest Revenue
SELECT 
  rental_date,
  daily_revenue
FROM daily_daily_metrics
ORDER BY daily_revenue DESC
LIMIT 1;


-- 7. Top Locations with Highest Rental Duration (Total Hours)
SELECT 
  pickup_location,
  location_name,
  vehicle_type,
  total_rental_hours
FROM vlm_vehicle_location_metrics
ORDER BY total_rental_hours DESC
LIMIT 5;
