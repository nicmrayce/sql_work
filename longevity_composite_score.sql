{% set from_dttm = from_dttm if from_dttm is defined else '2025-01-01' %}
{% set to_dttm   = to_dttm   if to_dttm   is defined else '2026-01-01' %}

WITH
vo2_daily AS (
  SELECT
    org_members_view.user_uuid,
    (vo2_maxes.timestamp AT TIME ZONE 'utc')::date AS record_date,
    AVG(vo2_maxes.value) AS vo2_day
  FROM vo2_maxes
  JOIN org_members_view ON vo2_maxes.user_id = org_members_view.user_id
  WHERE vo2_maxes.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
    AND {{ active_org_clients(org_uuid='f0174cf3-2d86-4857-839a-1cdf16ac9f0a') }}
  GROUP BY org_members_view.user_uuid, (vo2_maxes.timestamp AT TIME ZONE 'utc')::date
),
vo2_7 AS (
  SELECT
    user_uuid,
    record_date,
    AVG(vo2_day) OVER (PARTITION BY user_uuid ORDER BY record_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS vo2_7d
  FROM vo2_daily
),

body_daily AS (
  SELECT
    org_members_view.user_uuid,
    (body_fats.timestamp AT TIME ZONE 'utc')::date AS record_date,
    AVG(body_fats.value) AS bodyfat_day
  FROM body_fats
  JOIN org_members_view ON body_fats.user_id = org_members_view.user_id
  WHERE body_fats.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
    AND {{ active_org_clients(org_uuid='f0174cf3-2d86-4857-839a-1cdf16ac9f0a') }}
  GROUP BY org_members_view.user_uuid, (body_fats.timestamp AT TIME ZONE 'utc')::date
),
body_7 AS (
  SELECT
    user_uuid,
    record_date,
    AVG(bodyfat_day) OVER (PARTITION BY user_uuid ORDER BY record_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bodyfat_7d
  FROM body_daily
),

rhr_daily AS (
  SELECT
    org_members_view.user_uuid,
    resting_heart_rates.date::date AS record_date,
    AVG(resting_heart_rates.value) AS rhr_day
  FROM resting_heart_rates
  JOIN org_members_view ON resting_heart_rates.user_id = org_members_view.user_id
  WHERE resting_heart_rates.date BETWEEN '{{ from_dttm }}'::date AND '{{ to_dttm }}'::date
    AND {{ active_org_clients(org_uuid='f0174cf3-2d86-4857-839a-1cdf16ac9f0a') }}
  GROUP BY org_members_view.user_uuid, resting_heart_rates.date::date
),
rhr_7 AS (
  SELECT
    user_uuid,
    record_date,
    AVG(rhr_day) OVER (PARTITION BY user_uuid ORDER BY record_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rhr_7d
  FROM rhr_daily
),

dates AS (
  SELECT user_uuid, record_date FROM vo2_7
  UNION
  SELECT user_uuid, record_date FROM body_7
  UNION
  SELECT user_uuid, record_date FROM rhr_7
),

rolling AS (
  SELECT
    d.user_uuid,
    d.record_date::date AS record_date,
    v.vo2_7d,
    b.bodyfat_7d,
    r.rhr_7d
  FROM dates d
  LEFT JOIN vo2_7  v USING (user_uuid, record_date)
  LEFT JOIN body_7 b USING (user_uuid, record_date)
  LEFT JOIN rhr_7  r USING (user_uuid, record_date)
),

subscores AS (
  SELECT
    rolling.user_uuid,
    rolling.record_date,
    CASE
      WHEN rolling.vo2_7d IS NULL THEN NULL
      ELSE GREATEST(0, LEAST(100, (rolling.vo2_7d - 25) * (100.0 / (55 - 25))))
    END AS vo2_subscore,
    CASE
      WHEN rolling.bodyfat_7d IS NULL THEN NULL
      WHEN org_members_view.sex = 'male'   THEN GREATEST(0, LEAST(100, (30 - rolling.bodyfat_7d) * (100.0 / (30 - 8))))
      WHEN org_members_view.sex = 'female' THEN GREATEST(0, LEAST(100, (40 - rolling.bodyfat_7d) * (100.0 / (40 - 18))))
      ELSE NULL
    END AS bodyfat_subscore,
    CASE
      WHEN rolling.rhr_7d IS NULL THEN NULL
      ELSE GREATEST(0, LEAST(100, (90 - rolling.rhr_7d) * (100.0 / (90 - 50))))
    END AS rhr_subscore
  FROM rolling
  JOIN org_members_view ON rolling.user_uuid = org_members_view.user_uuid
),

composite AS (
  SELECT
    user_uuid,
    record_date,
    vo2_subscore,
    bodyfat_subscore,
    rhr_subscore,
    ROUND((
      COALESCE(vo2_subscore, 0)
      + COALESCE(bodyfat_subscore, 0)
      + COALESCE(rhr_subscore, 0)
    )::numeric
    / NULLIF(
        (CASE WHEN vo2_subscore    IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN bodyfat_subscore IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN rhr_subscore     IS NOT NULL THEN 1 ELSE 0 END), 0
      ), 0) AS longevity_composite_score
  FROM subscores
)

SELECT *
FROM composite
WHERE (vo2_subscore IS NOT NULL OR bodyfat_subscore IS NOT NULL OR rhr_subscore IS NOT NULL)
ORDER BY record_date, user_uuid;
