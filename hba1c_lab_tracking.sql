{% set from_dttm = from_dttm if from_dttm is defined else '2025-01-01' %}
{% set to_dttm   = to_dttm   if to_dttm   is defined else '2026-01-01' %}

{# Offsets from bound filters; default 0 #}
{% set baseline_off = (filter_values('baseline_offset_months')[0] | int) if filter_values('baseline_offset_months') else 0 %}
{% set latest_off   = (filter_values('latest_offset_months')[0]   | int) if filter_values('latest_offset_months') else 0 %}

{# Exact reading_count selector (multi-select). If none selected → include all #}
{% set rc_vals = filter_values('reading_count') %}
{% set rc_list = rc_vals | map('int') | list if rc_vals else [] %}

WITH window_params AS (
  SELECT
    (DATE '{{ from_dttm }}')::date AS base_from,
    (DATE '{{ to_dttm }}')::date   AS base_to,
    {{ baseline_off }}::int AS baseline_offset_months,
    {{ latest_off }}::int   AS latest_offset_months,
    ((DATE '{{ from_dttm }}') + ({{ baseline_off }} || ' month')::interval)::timestamp AS baseline_start,
    ((DATE '{{ to_dttm }}')   + ({{ latest_off }}   || ' month')::interval)::timestamp AS latest_end
),

base AS (
  SELECT
    u.id AS user_id,
    org_members_view.full_name,
    org_members_view.email,
    CAST(res.value AS DECIMAL)             AS component_value,
    CAST(bm.result_date_time AS TIMESTAMP) AS result_date_time,
    COALESCE(lc.name, lm.name, res.name)   AS mapped_component_name
  FROM lab_report_results res
  INNER JOIN biomarkers bm ON bm.uuid = res.biomarker_uuid
  INNER JOIN users u ON u.id = bm.user_id
  INNER JOIN org_members_view ON org_members_view.user_id = u.id
  LEFT JOIN lab_mappings lm
    ON res."name" = lm.name::text
   AND (lm.facility::text = bm.lab_provider OR lm.facility::text IS NULL)
  LEFT JOIN lab_components lc ON lm.lab_component_id = lc.id
  WHERE 1=1
    AND LOWER(COALESCE(lc.name, lm.name, res.name)) LIKE LOWER('%hba1c%')
    AND {{ active_org_clients(org_uuid='f0174cf3-2d86-4857-839a-1cdf16ac9f0a') }}
),

filtered AS (
  SELECT b.*, wp.baseline_start, wp.latest_end,
         wp.baseline_offset_months, wp.latest_offset_months,
         wp.base_from, wp.base_to
  FROM base b
  CROSS JOIN window_params wp
  WHERE b.result_date_time BETWEEN wp.baseline_start AND wp.latest_end
),

counts AS (
  SELECT user_id, COUNT(*) AS reading_count
  FROM filtered
  GROUP BY user_id
),

counts_filtered AS (
  SELECT c.*
  FROM counts c
  {% if rc_list and rc_list | length > 0 %}
    WHERE c.reading_count IN ({{ rc_list | join(', ') }})
  {% endif %}
),

eligible AS (
  SELECT DISTINCT cf.user_id FROM counts_filtered cf
),

ranked AS (
  SELECT
    f.*, 
    ROW_NUMBER() OVER (PARTITION BY f.user_id ORDER BY f.result_date_time ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY f.user_id ORDER BY f.result_date_time DESC) AS rn_desc
  FROM filtered f
  INNER JOIN eligible e ON f.user_id = e.user_id
),

paired AS (
  SELECT
    r.user_id,
    r.full_name,
    r.email,
    r.mapped_component_name,
    MAX(CASE WHEN r.rn_asc  = 1 THEN r.component_value   END) AS baseline_value,
    MAX(CASE WHEN r.rn_desc = 1 THEN r.component_value   END) AS latest_value,
    MAX(CASE WHEN r.rn_asc  = 1 THEN r.result_date_time  END) AS baseline_date,
    MAX(CASE WHEN r.rn_desc = 1 THEN r.result_date_time  END) AS latest_date,
    STDDEV(r.component_value) AS per_patient_stddev,
    MAX(cf.reading_count)     AS reading_count,
    MAX(r.baseline_offset_months) AS baseline_offset_months,
    MAX(r.latest_offset_months)   AS latest_offset_months,
    MAX(r.base_from) AS base_from,
    MAX(r.base_to)   AS base_to
  FROM ranked r
  INNER JOIN counts_filtered cf ON r.user_id = cf.user_id
  GROUP BY r.user_id, r.full_name, r.email, r.mapped_component_name
),

pct_change_per_patient AS (
  SELECT
    p.*,
    CASE
      WHEN p.baseline_value IS NOT NULL AND p.baseline_value <> 0
      THEN (p.latest_value - p.baseline_value) / p.baseline_value
      ELSE NULL
    END AS pct_change,
    (
      EXTRACT(YEAR  FROM AGE(p.baseline_date::date, p.base_from))::int * 12 +
      EXTRACT(MONTH FROM AGE(p.baseline_date::date, p.base_from))::int
    ) AS baseline_month_delta,
    (
      EXTRACT(YEAR  FROM AGE(p.latest_date::date, p.base_to))::int * 12 +
      EXTRACT(MONTH FROM AGE(p.latest_date::date, p.base_to))::int
    ) AS latest_month_delta
  FROM paired p
),

filtered_patient AS (
  SELECT * FROM pct_change_per_patient
  {% if rc_list and rc_list | length > 0 %}
    WHERE reading_count IN ({{ rc_list | join(', ') }})
  {% endif %}
),

sample_meta AS (
  SELECT COUNT(DISTINCT user_id) AS sample_size FROM filtered_patient
)

-- Unified output
SELECT * FROM (

--Temporal
SELECT
  fp.user_id,
  fp.full_name,
  fp.mapped_component_name,
  fp.latest_date                         AS result_date_time,
  AVG(fp.pct_change)                     AS avg_pct_change,
  AVG(fp.latest_value - fp.baseline_value) AS raw_change,
  -- NULL::FLOAT                            AS raw_change,
  fp.baseline_value,
  fp.latest_value,
  sm.sample_size,
  STDDEV(fp.pct_change)                  AS stddev_pct_change,
  fp.baseline_date,
  fp.latest_date,
  fp.reading_count,
  fp.baseline_month_delta,
  fp.latest_month_delta,
  fp.baseline_offset_months,
  fp.latest_offset_months,
  'temporal'                             AS view_type
FROM filtered_patient fp
CROSS JOIN sample_meta sm
GROUP BY fp.user_id, fp.full_name, fp.mapped_component_name, fp.latest_date,
         fp.baseline_value, fp.latest_value, fp.per_patient_stddev,
         fp.baseline_date, fp.reading_count,
         fp.baseline_month_delta, fp.latest_month_delta,
         fp.baseline_offset_months, fp.latest_offset_months, sm.sample_size

UNION ALL

--Summary
SELECT
  NULL::integer                          AS user_id,
  NULL::TEXT                             AS full_name,
  fp.mapped_component_name,
  MAX(fp.latest_date)                    AS result_date_time,
  AVG(fp.pct_change)                     AS avg_pct_change,
  AVG(fp.latest_value - fp.baseline_value) AS raw_change,
  -- NULL::FLOAT                            AS raw_change,
  NULL::FLOAT                            AS baseline_value,
  NULL::FLOAT                            AS latest_value,
  sm.sample_size,
  STDDEV(fp.pct_change)                  AS stddev_pct_change,
  NULL::TIMESTAMP                        AS baseline_date,
  MAX(fp.latest_date)                    AS latest_date,
  MAX(fp.reading_count)                  AS reading_count,
  MAX(fp.baseline_month_delta)           AS baseline_month_delta,
  MAX(fp.latest_month_delta)             AS latest_month_delta,
  MAX(fp.baseline_offset_months)         AS baseline_offset_months,
  MAX(fp.latest_offset_months)           AS latest_offset_months,
  'summary'                              AS view_type
FROM filtered_patient fp
CROSS JOIN sample_meta sm
GROUP BY fp.mapped_component_name, sm.sample_size

UNION ALL

--Detail
SELECT
  fp.user_id,
  fp.full_name,
  fp.mapped_component_name,
  fp.latest_date                         AS result_date_time,
  fp.pct_change                          AS avg_pct_change,
  (fp.latest_value - fp.baseline_value)  AS raw_change,
  fp.baseline_value,
  fp.latest_value,
  sm.sample_size,
  fp.per_patient_stddev                  AS stddev_pct_change,
  fp.baseline_date,
  fp.latest_date,
  fp.reading_count,
  fp.baseline_month_delta,
  fp.latest_month_delta,
  fp.baseline_offset_months,
  fp.latest_offset_months,
  'detail'                               AS view_type
FROM filtered_patient fp
CROSS JOIN sample_meta sm

) final
ORDER BY mapped_component_name, result_date_time;
