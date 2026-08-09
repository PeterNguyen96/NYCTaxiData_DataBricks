-- NYC Yellow Taxi Dashboard — backing queries
-- Run these against the `nyc_taxi` database created by the notebooks in /notebooks.
-- Each query maps to one tile on the Lakeview dashboard (see dashboard/README.md).

-- ============================================================
-- KPI row
-- ============================================================

-- Total trips
SELECT SUM(total_trips) AS total_trips FROM nyc_taxi.gold_daily_summary;

-- Total revenue
SELECT ROUND(SUM(total_revenue), 0) AS total_revenue FROM nyc_taxi.gold_daily_summary;

-- Average fare
SELECT ROUND(AVG(avg_fare), 2) AS avg_fare FROM nyc_taxi.gold_daily_summary;

-- Average tip %
SELECT ROUND(AVG(avg_tip_pct), 2) AS avg_tip_pct FROM nyc_taxi.gold_daily_summary;

-- ============================================================
-- Trend: trips & revenue by day (line chart)
-- ============================================================

SELECT pickup_date, total_trips, total_revenue
FROM nyc_taxi.gold_daily_summary
ORDER BY pickup_date;

-- ============================================================
-- Demand by hour of day (bar/line chart)
-- ============================================================

SELECT pickup_hour, total_trips, avg_fare, avg_tip_pct
FROM nyc_taxi.gold_hourly_pattern
ORDER BY pickup_hour;

-- ============================================================
-- Demand by day of week (bar chart)
-- ============================================================

SELECT pickup_day_of_week, is_weekend, total_trips, avg_fare
FROM nyc_taxi.gold_day_of_week_pattern
ORDER BY
  CASE pickup_day_of_week
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
  END;

-- ============================================================
-- Top pickup zones (bar chart / table)
-- ============================================================

SELECT pickup_borough, pickup_zone, total_trips, total_revenue, avg_fare
FROM nyc_taxi.gold_zone_summary
ORDER BY total_trips DESC
LIMIT 15;

-- Revenue by borough (pie / donut chart)
SELECT pickup_borough, SUM(total_revenue) AS total_revenue, SUM(total_trips) AS total_trips
FROM nyc_taxi.gold_zone_summary
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY total_revenue DESC;

-- ============================================================
-- Payment type breakdown (pie chart)
-- ============================================================

SELECT payment_type_label, total_trips, pct_of_total_trips, avg_tip_pct
FROM nyc_taxi.gold_payment_summary
ORDER BY total_trips DESC;

-- ============================================================
-- Trip distance distribution (bar chart)
-- ============================================================

SELECT distance_bucket, total_trips, avg_fare, avg_duration_min
FROM nyc_taxi.gold_distance_buckets
ORDER BY
  CASE distance_bucket
    WHEN '0-1 mi' THEN 1 WHEN '1-3 mi' THEN 2 WHEN '3-6 mi' THEN 3
    WHEN '6-10 mi' THEN 4 WHEN '10+ mi' THEN 5
  END;
