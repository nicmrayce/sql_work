-- ============================================
-- Longevity Score 
-- 20 metrics -> 0–100 normalization -> weighted
-- Final Score scaled to 300–950
-- ============================================

WITH date_parameters AS (
  SELECT
    365 AS lab_lookback_days,   -- labs 1y
    90  AS wearable_lookback_days -- wearables/tiles 90d
),

filtered_members AS (
  SELECT
    org_members_view.user_id,
    org_members_view.organization_uuid,
    org_members_view.full_name,
    org_members_view.username,
    org_members_view.sex,   -- 'male' / 'female'
    org_members_view.dob,
    DATE_PART('year', AGE(CURRENT_DATE, org_members_view.dob))::int AS age_years
  FROM org_members_view
  WHERE 1=1
  --AND {{ active_org_clients() }} 
  AND {{ active_org_clients(org_uuid='0d86d0c2-4b5f-4566-bcf4-a969a5b0e75a') }}
  
),

-- ------------------------
-- WEIGHTS (percent points)
-- ------------------------
weights AS (
  SELECT
    14::numeric AS w_ogtt,
    14::numeric AS w_apob,
    14::numeric AS w_vo2,
    10::numeric AS w_crp,
    10::numeric AS w_bmi,
    10::numeric AS w_packyears,
    10::numeric AS w_moca,
    14::numeric AS w_mvpa,
     9::numeric AS w_cac,
     5::numeric AS w_hrv,
     5::numeric AS w_phq9,
     5::numeric AS w_alt,
     5::numeric AS w_egfr,
     5::numeric AS w_bmd,
     5::numeric AS w_truage,
     5::numeric AS w_small_hdl,
     5::numeric AS w_rem,
     9::numeric AS w_grip,
    10::numeric AS w_swls,
    10::numeric AS w_hei
),

-- ------------------------
-- LAB COMPONENTS (1y)
-- ------------------------
recent_lab_values AS (
  SELECT DISTINCT ON (fm.user_id, COALESCE(lc.name, lrr.name))
    fm.user_id,
    COALESCE(lc.name, lrr.name) AS component_name,
    lrr.value,
    lrr.value_string,
    lrr.unit,
    b.result_date_time::timestamp AS result_dt
  FROM filtered_members fm
  CROSS JOIN date_parameters dp
  JOIN biomarkers b ON b.user_id = fm.user_id
  JOIN lab_report_results lrr ON lrr.biomarker_uuid = b.uuid
  LEFT JOIN lab_mappings lm ON lrr.name = lm.name
      AND (lm.facility = b.lab_provider OR lm.facility IS NULL)
  LEFT JOIN lab_components lc ON lm.lab_component_id = lc.id
  WHERE b.status = 'created'
    AND b.result_date_time::timestamp >= CURRENT_DATE - (dp.lab_lookback_days || ' days')::interval
    AND COALESCE(lc.name, lrr.name) IN (
      'Glucose, 2 Hour (GTT)',
      'Apolipoprotein B (ApoB)',
      'VO2 Max',
      'C-Reactive Protein, Quantitative (CRP)', 'hs-CRP',
      'Body Mass Index',
      'Cardiac Calcium Score (Heart Disease Risk)',
      'ALT (Alanine Amino Transferase)',
      'eGFR',
      'InBody - Bone Mineral Content',
      'Biological Age',
      'Alpha-4 (Small HDL Particles)',
      -- Optional: if you have a direct BMD T-score component, include its exact name here:
      'Bone Mineral Density T-Score'
    )
  ORDER BY fm.user_id, COALESCE(lc.name, lrr.name), b.result_date_time DESC
),

lab_pivot AS (
  SELECT
    user_id,
    MAX(CASE WHEN component_name = 'Glucose, 2 Hour (GTT)' THEN value END) AS ogtt_2h_mgdl,
    MAX(CASE WHEN component_name = 'Apolipoprotein B (ApoB)' THEN value END) AS apob_mgdl,
    MAX(CASE WHEN component_name = 'VO2 Max' THEN value END) AS vo2_max,
    MAX(CASE WHEN component_name IN ('C-Reactive Protein, Quantitative (CRP)','hs-CRP') THEN value END) AS crp_mgl,
    MAX(CASE WHEN component_name = 'Body Mass Index' THEN value END) AS bmi,
    MAX(CASE WHEN component_name = 'Cardiac Calcium Score (Heart Disease Risk)' THEN value END) AS cac,
    MAX(CASE WHEN component_name = 'ALT (Alanine Amino Transferase)' THEN value END) AS alt_uL,
    MAX(CASE WHEN component_name = 'eGFR' THEN value END) AS egfr,
    MAX(CASE WHEN component_name = 'InBody - Bone Mineral Content' THEN value END) AS inbody_bmc, -- proxy if no BMD
    MAX(CASE WHEN component_name = 'Bone Mineral Density T-Score' THEN value END) AS bmd_tscore,  -- preferred
    MAX(CASE WHEN component_name = 'Biological Age' THEN value END) AS biological_age,
    MAX(CASE WHEN component_name = 'Alpha-4 (Small HDL Particles)' THEN value END) AS small_hdl,
    MAX(result_dt) AS lab_latest_dt
  FROM recent_lab_values
  GROUP BY user_id
),

-- ------------------------
-- BMI (table fallback, 1y)
-- ------------------------
recent_bmi_table AS (
  SELECT DISTINCT ON (fm.user_id)
    fm.user_id,
    b.value AS bmi_value,
    b.timestamp::timestamp AS bmi_date
  FROM filtered_members fm
  CROSS JOIN date_parameters dp
  JOIN bmis b ON b.user_id = fm.user_id
  WHERE b.timestamp >= CURRENT_DATE - (dp.lab_lookback_days || ' days')::interval
    AND b.value IS NOT NULL
  ORDER BY fm.user_id, b.timestamp DESC
),

-- ------------------------
-- WEARABLES / TILES (90d)
-- ------------------------

-- HRV (RMSSD ms) tile
hrv_tile AS (
  SELECT
    fm.user_id,
    AVG(r.value)::numeric AS hrv_rmssd
  FROM filtered_members fm
  CROSS JOIN date_parameters dp
  JOIN rmssds r ON r.user_id = fm.user_id
  WHERE r.timestamp >= CURRENT_DATE - (dp.wearable_lookback_days || ' days')::interval
  GROUP BY fm.user_id
),

-- Sleep REM% from sleeps (compute pct if not stored)
-- Assumes sleeps table with JSON or columns: rem (sec), duration (sec)
rem_sleep AS (
  SELECT
    fm.user_id,
    AVG(
      CASE
        WHEN (s.data_json ->> 'duration')::numeric > 0
          THEN ((s.data_json ->> 'rem')::numeric / (s.data_json ->> 'duration')::numeric) * 100.0
        WHEN s.duration IS NOT NULL AND s.duration > 0 AND s.rem IS NOT NULL
          THEN (s.rem::numeric / s.duration::numeric) * 100.0
        ELSE NULL
      END
    )::numeric AS rem_pct
  FROM filtered_members fm
  CROSS JOIN date_parameters dp
  JOIN sleeps s ON s.user_id = fm.user_id
  WHERE s.day >= CURRENT_DATE - (dp.wearable_lookback_days || ' days')::interval
  GROUP BY fm.user_id
),

-- MVPA (sum moderate + vigorous) from Oura activity detail / steps JSON in seconds
mvpa_week AS (
  SELECT
    fm.user_id,
    -- average daily MVPA minutes * 7 = weekly minutes
    AVG(
      (COALESCE((s.data_json ->> 'medium_activity_time')::numeric, 0)
     + COALESCE((s.data_json ->> 'high_activity_time')::numeric, 0)) / 60.0
    ) * 7.0 AS mvpa_weekly_min
  FROM filtered_members fm
  CROSS JOIN date_parameters dp
  JOIN steps s ON s.user_id = fm.user_id
  WHERE s.source = 'oura'
    AND s.date >= CURRENT_DATE - (dp.wearable_lookback_days || ' days')::interval
  GROUP BY fm.user_id
),

-- ------------------------
-- ASSESSMENTS / TILES (most recent in 1y unless noted)
-- ------------------------

-- MoCA (Montreal Cognitive Assessment, 0–30)
moca_tile AS (
  SELECT
    fm.user_id,
    MAX((sm.metadata->>'moca_score')::numeric) AS moca_score
  FROM filtered_members fm
  JOIN source_metrics sm ON sm.user_id = fm.user_id
  WHERE sm.data_source_url = 'quotient_score'
    AND sm.metadata ? 'moca_score'
  GROUP BY fm.user_id
),

-- PHQ-9 (Patient Health Questionnaire, 0–27)
phq9_assessment AS (
  SELECT
    fm.user_id,
    MAX((sm.metadata->>'phq_9_score')::numeric) AS phq9_score
  FROM filtered_members fm
  JOIN source_metrics sm ON sm.user_id = fm.user_id
  WHERE sm.data_source_url = 'quotient_score'
    AND sm.metadata ? 'phq_9_score'
  GROUP BY fm.user_id
),


-- Pack-Years (Smoking Exposure)
packyears_tile AS (
  SELECT
    fm.user_id,
    MAX((sm.metadata->>'smoking_years')::numeric) AS pack_years
  FROM filtered_members fm
  JOIN source_metrics sm ON sm.user_id = fm.user_id
  WHERE sm.data_source_url = 'quotient_score'
    AND sm.metadata ? 'smoking_years'
  GROUP BY fm.user_id
),

-- SWLS (Satisfaction With Life Scale, 5–35)
swls_tile AS (
  SELECT
    fm.user_id,
    MAX((sm.metadata->>'swls_score')::numeric) AS swls_score
  FROM filtered_members fm
  JOIN source_metrics sm ON sm.user_id = fm.user_id
  WHERE sm.data_source_url = 'quotient_score'
    AND sm.metadata ? 'swls_score'
  GROUP BY fm.user_id
),

-- HEI-2015 (Healthy Eating Index, 0–100)
hei_tile AS (
  SELECT
    fm.user_id,
    MAX((sm.metadata->>'hei_score')::numeric) AS hei_score
  FROM filtered_members fm
  JOIN source_metrics sm ON sm.user_id = fm.user_id
  WHERE sm.data_source_url = 'quotient_score'
    AND sm.metadata ? 'hei_score'
  GROUP BY fm.user_id
),

-- Grip Strength (lbs in Heads Up -> convert to kg);
-- Use the best of left/right latest; or average—here we use MAX of the latest test.
-- Grip Strength from source_metrics (left_grip/right_grip) → best (kg)
grip_strength AS (
  WITH side_latest AS (
    SELECT
      fm.user_id,
      sm.data_source_url,
      sm.value::numeric AS value_lbs,
      sm.timestamp,
      ROW_NUMBER() OVER (
        PARTITION BY fm.user_id, sm.data_source_url
        ORDER BY sm.timestamp DESC
      ) AS rn
    FROM filtered_members fm
    JOIN source_metrics sm ON sm.user_id = fm.user_id
    WHERE sm.timestamp >= CURRENT_DATE - INTERVAL '365 days'
      AND sm.data_source_url IN ('left_grip','right_grip')
  ),
  latest_per_side AS (
    SELECT
      user_id,
      MAX(value_lbs) FILTER (WHERE data_source_url = 'left_grip'  AND rn = 1) AS left_lbs,
      MAX(value_lbs) FILTER (WHERE data_source_url = 'right_grip' AND rn = 1) AS right_lbs
    FROM side_latest
    GROUP BY user_id
  )
  SELECT
    user_id,
    (GREATEST(COALESCE(left_lbs,0), COALESCE(right_lbs,0)) * 0.45359237)::numeric AS grip_kg
  FROM latest_per_side
),

-- ------------------------
-- Merge all values
-- ------------------------
raw_values AS (
  SELECT
    fm.user_id,
    fm.full_name, fm.username, fm.sex, fm.age_years,

    -- Labs
    lp.ogtt_2h_mgdl,
    lp.apob_mgdl,
    lp.vo2_max,
    lp.crp_mgl,
    COALESCE(lp.bmi, bmi_tbl.bmi_value) AS bmi,
    lp.cac,
    lp.alt_uL,
    lp.egfr,
    COALESCE(lp.bmd_tscore, NULL) AS bmd_tscore, -- prefer T-score
    lp.inbody_bmc,                               -- proxy only if you decide to use it
    lp.biological_age,
    lp.small_hdl,

    -- Wearables/Tiles
    hrv.hrv_rmssd,
    rem.rem_pct,
    mvpa.mvpa_weekly_min,

    -- Assessments/Tiles
    mo.moca_score,
    ph.phq9_score,
    pk.pack_years,
    sw.swls_score,
    he.hei_score,

    -- Grip
    gs.grip_kg,
    
    GREATEST(
      COALESCE(lp.lab_latest_dt, '1900-01-01'::timestamp),
      COALESCE(bmi_tbl.bmi_date, '1900-01-01'::timestamp),
      COALESCE((SELECT MAX(r.timestamp) FROM rmssds r WHERE r.user_id = fm.user_id), '1900-01-01'::timestamp),
      COALESCE((SELECT MAX(s.day) FROM sleeps s WHERE s.user_id = fm.user_id), '1900-01-01'::timestamp),
      COALESCE((SELECT MAX(st.date) FROM steps st WHERE st.user_id = fm.user_id), '1900-01-01'::timestamp),
      COALESCE((SELECT MAX(sm.timestamp) FROM source_metrics sm WHERE sm.user_id = fm.user_id), '1900-01-01'::timestamp)
    ) AS data_timestamp

  FROM filtered_members fm
  LEFT JOIN lab_pivot       lp  ON lp.user_id = fm.user_id
  LEFT JOIN recent_bmi_table bmi_tbl ON bmi_tbl.user_id = fm.user_id
  LEFT JOIN hrv_tile        hrv ON hrv.user_id = fm.user_id
  LEFT JOIN rem_sleep       rem ON rem.user_id = fm.user_id
  LEFT JOIN mvpa_week       mvpa ON mvpa.user_id = fm.user_id
  LEFT JOIN moca_tile       mo  ON mo.user_id  = fm.user_id
  LEFT JOIN phq9_assessment ph  ON ph.user_id  = fm.user_id
  LEFT JOIN packyears_tile  pk  ON pk.user_id  = fm.user_id
  LEFT JOIN swls_tile       sw  ON sw.user_id  = fm.user_id
  LEFT JOIN hei_tile        he  ON he.user_id  = fm.user_id
  LEFT JOIN grip_strength   gs  ON gs.user_id  = fm.user_id
),

-- ------------------------
-- Normalize each metric to 0–100 (higher = better)
-- These bands are intentionally explicit for easy tuning.
-- ------------------------
normalized AS (
  SELECT
    rv.*,

    -- 2-Hour OGTT (mg/dL; lower is better)
    CASE
      WHEN rv.ogtt_2h_mgdl IS NULL THEN NULL
      WHEN rv.ogtt_2h_mgdl < 120 THEN 95
      WHEN rv.ogtt_2h_mgdl BETWEEN 120 AND 139 THEN 82
      WHEN rv.ogtt_2h_mgdl BETWEEN 140 AND 199 THEN 60
      ELSE 35
    END AS ogtt_score_100,

    -- ApoB (mg/dL; lower is better)
    CASE
      WHEN rv.apob_mgdl IS NULL THEN NULL
      WHEN rv.apob_mgdl < 70 THEN 95
      WHEN rv.apob_mgdl BETWEEN 70 AND 89 THEN 82
      WHEN rv.apob_mgdl BETWEEN 90 AND 109 THEN 62
      ELSE 40
    END AS apob_score_100,

    -- VO2 Max (mL/kg/min; age/sex bands)
    CASE
      WHEN rv.vo2_max IS NULL THEN NULL
      ELSE
        CASE
          -- MEN
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 18 AND 19 THEN
            CASE WHEN rv.vo2_max >= 58 THEN 95 WHEN rv.vo2_max >= 50 THEN 82 WHEN rv.vo2_max >= 46 THEN 67 WHEN rv.vo2_max >= 38 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 20 AND 29 THEN
            CASE WHEN rv.vo2_max >= 56 THEN 95 WHEN rv.vo2_max >= 49 THEN 82 WHEN rv.vo2_max >= 43 THEN 67 WHEN rv.vo2_max >= 36 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 30 AND 39 THEN
            CASE WHEN rv.vo2_max >= 53 THEN 95 WHEN rv.vo2_max >= 46 THEN 82 WHEN rv.vo2_max >= 40 THEN 67 WHEN rv.vo2_max >= 35 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 40 AND 49 THEN
            CASE WHEN rv.vo2_max >= 52 THEN 95 WHEN rv.vo2_max >= 44 THEN 82 WHEN rv.vo2_max >= 39 THEN 67 WHEN rv.vo2_max >= 34 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 50 AND 59 THEN
            CASE WHEN rv.vo2_max >= 50 THEN 95 WHEN rv.vo2_max >= 41 THEN 82 WHEN rv.vo2_max >= 36 THEN 67 WHEN rv.vo2_max >= 29 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 60 AND 69 THEN
            CASE WHEN rv.vo2_max >= 46 THEN 95 WHEN rv.vo2_max >= 36 THEN 82 WHEN rv.vo2_max >= 30 THEN 67 WHEN rv.vo2_max >= 25 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years BETWEEN 70 AND 79 THEN
            CASE WHEN rv.vo2_max >= 41 THEN 95 WHEN rv.vo2_max >= 30 THEN 82 WHEN rv.vo2_max >= 25 THEN 67 WHEN rv.vo2_max >= 21 THEN 50 ELSE 20 END
          WHEN rv.sex = 'male' AND rv.age_years >= 80 THEN
            CASE WHEN rv.vo2_max >= 36 THEN 95 WHEN rv.vo2_max >= 26 THEN 82 WHEN rv.vo2_max >= 23 THEN 67 WHEN rv.vo2_max >= 18 THEN 50 ELSE 20 END
          -- WOMEN
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 18 AND 19 THEN
            CASE WHEN rv.vo2_max >= 53 THEN 95 WHEN rv.vo2_max >= 46 THEN 82 WHEN rv.vo2_max >= 40 THEN 67 WHEN rv.vo2_max >= 35 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 20 AND 29 THEN
            CASE WHEN rv.vo2_max >= 51 THEN 95 WHEN rv.vo2_max >= 41 THEN 82 WHEN rv.vo2_max >= 36 THEN 67 WHEN rv.vo2_max >= 28 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 30 AND 39 THEN
            CASE WHEN rv.vo2_max >= 49 THEN 95 WHEN rv.vo2_max >= 39 THEN 82 WHEN rv.vo2_max >= 34 THEN 67 WHEN rv.vo2_max >= 27 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 40 AND 49 THEN
            CASE WHEN rv.vo2_max >= 47 THEN 95 WHEN rv.vo2_max >= 37 THEN 82 WHEN rv.vo2_max >= 32 THEN 67 WHEN rv.vo2_max >= 26 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 50 AND 59 THEN
            CASE WHEN rv.vo2_max >= 46 THEN 95 WHEN rv.vo2_max >= 36 THEN 82 WHEN rv.vo2_max >= 29 THEN 67 WHEN rv.vo2_max >= 25 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 60 AND 69 THEN
            CASE WHEN rv.vo2_max >= 40 THEN 95 WHEN rv.vo2_max >= 30 THEN 82 WHEN rv.vo2_max >= 25 THEN 67 WHEN rv.vo2_max >= 21 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years BETWEEN 70 AND 79 THEN
            CASE WHEN rv.vo2_max >= 36 THEN 95 WHEN rv.vo2_max >= 25 THEN 82 WHEN rv.vo2_max >= 22 THEN 67 WHEN rv.vo2_max >= 18 THEN 50 ELSE 20 END
          WHEN rv.sex = 'female' AND rv.age_years >= 80 THEN
            CASE WHEN rv.vo2_max >= 30 THEN 95 WHEN rv.vo2_max >= 23 THEN 82 WHEN rv.vo2_max >= 20 THEN 67 WHEN rv.vo2_max >= 15 THEN 50 ELSE 20 END
          ELSE 50
        END
    END AS vo2_score_100,

    -- CRP (mg/L; lower is better)
    CASE
      WHEN rv.crp_mgl IS NULL THEN NULL
      WHEN rv.crp_mgl < 1.0 THEN 95
      WHEN rv.crp_mgl <= 3.0 THEN 82
      ELSE 70
    END AS crp_score_100,

    -- BMI (18.5–24.9 optimal; lower/higher worse)
    CASE
      WHEN rv.bmi IS NULL THEN NULL
      WHEN rv.bmi < 18.5 THEN 50
      WHEN rv.bmi < 25   THEN 95
      WHEN rv.bmi < 30   THEN 82
      WHEN rv.bmi < 35   THEN 62
      WHEN rv.bmi < 40   THEN 40
      ELSE 20
    END AS bmi_score_100,

    -- Smoking Pack-Years (lower is better)
    CASE
      WHEN rv.pack_years IS NULL THEN NULL
      WHEN rv.pack_years = 0          THEN 100
      WHEN rv.pack_years <= 5         THEN 80
      WHEN rv.pack_years <= 10        THEN 65
      WHEN rv.pack_years <= 20        THEN 50
      ELSE 30
    END AS packyears_score_100,

    -- MoCA (0–30; higher is better)
    CASE
      WHEN rv.moca_score IS NULL THEN NULL
      WHEN rv.moca_score >= 30 THEN 100
      WHEN rv.moca_score <= 0  THEN 0
      ELSE (rv.moca_score / 30.0) * 100.0
    END AS moca_score_100,

    -- MVPA weekly minutes (higher is better)
    CASE
      WHEN rv.mvpa_weekly_min IS NULL THEN NULL
      WHEN rv.mvpa_weekly_min >= 300 THEN 95
      WHEN rv.mvpa_weekly_min >= 150 THEN 82
      WHEN rv.mvpa_weekly_min >= 100 THEN 70
      WHEN rv.mvpa_weekly_min >= 75  THEN 62
      WHEN rv.mvpa_weekly_min >= 30  THEN 50
      ELSE 30
    END AS mvpa_score_100,

    -- CAC (Agatston; lower is better)
    CASE
      WHEN rv.cac IS NULL THEN NULL
      WHEN rv.cac = 0          THEN 95
      WHEN rv.cac <= 10        THEN 90
      WHEN rv.cac <= 100       THEN 70
      WHEN rv.cac <= 300       THEN 50
      ELSE 30
    END AS cac_score_100,

    -- HRV RMSSD (ms; higher is better; simple bands)
    CASE
      WHEN rv.hrv_rmssd IS NULL THEN NULL
      WHEN rv.hrv_rmssd > 70  THEN 95
      WHEN rv.hrv_rmssd >= 50 THEN 82
      WHEN rv.hrv_rmssd >= 30 THEN 62
      ELSE 40
    END AS hrv_score_100,

    -- PHQ-9 (0–27; lower is better) → invert to 0–100
    CASE
      WHEN rv.phq9_score IS NULL THEN NULL
      WHEN rv.phq9_score <= 0 THEN 100
      WHEN rv.phq9_score >= 27 THEN 0
      ELSE ((27.0 - rv.phq9_score) / 27.0) * 100.0
    END AS phq9_score_100,

    -- ALT (U/L; lower better; pragmatic bands)
    CASE
      WHEN rv.alt_uL IS NULL THEN NULL
      WHEN rv.alt_uL <= 40  THEN 95
      WHEN rv.alt_uL <= 60  THEN 82
      WHEN rv.alt_uL <= 100 THEN 62
      ELSE 40
    END AS alt_score_100,

    -- eGFR (mL/min/1.73m²; higher better)
    CASE
      WHEN rv.egfr IS NULL THEN NULL
      WHEN rv.egfr >= 90 THEN 95
      WHEN rv.egfr >= 60 THEN 82
      WHEN rv.egfr >= 45 THEN 62
      WHEN rv.egfr >= 30 THEN 40
      ELSE 20
    END AS egfr_score_100,

    -- Bone Mineral Density: Prefer T-score bands; else leave NULL (avoid proxy unless you approve)
    CASE
      WHEN rv.bmd_tscore IS NULL THEN NULL
      WHEN rv.bmd_tscore >= -1.0         THEN 95  -- normal
      WHEN rv.bmd_tscore >= -2.5         THEN 70  -- osteopenia
      ELSE 40                                   -- osteoporosis
    END AS bmd_score_100,

    -- TruAge (epigenetic) : use Biological Age delta = bio_age - chronological (lower is better)
    CASE
      WHEN rv.biological_age IS NULL OR rv.age_years IS NULL THEN NULL
      ELSE rv.biological_age - rv.age_years
    END AS truage_delta_years,
    CASE
      WHEN rv.biological_age IS NULL OR rv.age_years IS NULL THEN NULL
      WHEN (rv.biological_age - rv.age_years) <= -5 THEN 95
      WHEN (rv.biological_age - rv.age_years) <= -2 THEN 82
      WHEN ABS(rv.biological_age - rv.age_years) <= 2 THEN 67
      WHEN (rv.biological_age - rv.age_years) <= 5  THEN 50
      ELSE 30
    END AS truage_score_100,

    -- Small HDL Particles (units vary; placeholder positive direction; tune cutpoints to your lab ranges)
    CASE
      WHEN rv.small_hdl IS NULL THEN NULL
      -- TODO: replace with your validated reference bands (higher assumed better here)
      WHEN rv.small_hdl >= 75 THEN 95
      WHEN rv.small_hdl >= 60 THEN 82
      WHEN rv.small_hdl >= 45 THEN 62
      ELSE 40
    END AS small_hdl_score_100,

    -- REM Sleep % (optimal ~20–25%)
    CASE
      WHEN rv.rem_pct IS NULL THEN NULL
      WHEN rv.rem_pct BETWEEN 20 AND 25 THEN 95
      WHEN rv.rem_pct BETWEEN 17 AND 19.99 THEN 82
      WHEN rv.rem_pct BETWEEN 25.01 AND 27.99 THEN 82
      WHEN rv.rem_pct BETWEEN 14 AND 16.99 THEN 62
      WHEN rv.rem_pct BETWEEN 28 AND 30.99 THEN 62
      ELSE 40
    END AS rem_score_100,

    -- Grip Strength (kg; age/sex pragmatic cutpoints)
    CASE
      WHEN rv.grip_kg IS NULL THEN NULL
      WHEN rv.sex = 'male' THEN
        CASE
          WHEN rv.grip_kg >= 60 THEN 95
          WHEN rv.grip_kg >= 50 THEN 82
          WHEN rv.grip_kg >= 40 THEN 67
          WHEN rv.grip_kg >= 30 THEN 50
          ELSE 35
        END
      WHEN rv.sex = 'female' THEN
        CASE
          WHEN rv.grip_kg >= 40 THEN 95
          WHEN rv.grip_kg >= 35 THEN 82
          WHEN rv.grip_kg >= 25 THEN 67
          WHEN rv.grip_kg >= 20 THEN 50
          ELSE 35
        END
      ELSE 65
    END AS grip_score_100,

    -- SWLS (5–35; higher better) -> 0–100
    CASE
      WHEN rv.swls_score IS NULL THEN NULL
      WHEN rv.swls_score <= 5  THEN 0
      WHEN rv.swls_score >= 35 THEN 100
      ELSE ((rv.swls_score - 5.0) / 30.0) * 100.0
    END AS swls_score_100,

    -- HEI-2015 (0–100; higher better)
    CASE
      WHEN rv.hei_score IS NULL THEN NULL
      WHEN rv.hei_score < 0   THEN 0
      WHEN rv.hei_score > 100 THEN 100
      ELSE rv.hei_score
    END AS hei_score_100
  FROM raw_values rv
),

-- ------------------------
-- Weighted aggregation
-- ------------------------
scored AS (
  SELECT
    n.*,
    w.*,

    -- Sum of weights present (only metrics that are non-null)
    (
      (CASE WHEN n.ogtt_score_100       IS NOT NULL THEN w.w_ogtt       ELSE 0 END) +
      (CASE WHEN n.apob_score_100       IS NOT NULL THEN w.w_apob       ELSE 0 END) +
      (CASE WHEN n.vo2_score_100        IS NOT NULL THEN w.w_vo2        ELSE 0 END) +
      (CASE WHEN n.crp_score_100        IS NOT NULL THEN w.w_crp        ELSE 0 END) +
      (CASE WHEN n.bmi_score_100        IS NOT NULL THEN w.w_bmi        ELSE 0 END) +
      (CASE WHEN n.packyears_score_100  IS NOT NULL THEN w.w_packyears  ELSE 0 END) +
      (CASE WHEN n.moca_score_100       IS NOT NULL THEN w.w_moca       ELSE 0 END) +
      (CASE WHEN n.mvpa_score_100       IS NOT NULL THEN w.w_mvpa       ELSE 0 END) +
      (CASE WHEN n.cac_score_100        IS NOT NULL THEN w.w_cac        ELSE 0 END) +
      (CASE WHEN n.hrv_score_100        IS NOT NULL THEN w.w_hrv        ELSE 0 END) +
      (CASE WHEN n.phq9_score_100       IS NOT NULL THEN w.w_phq9       ELSE 0 END) +
      (CASE WHEN n.alt_score_100        IS NOT NULL THEN w.w_alt        ELSE 0 END) +
      (CASE WHEN n.egfr_score_100       IS NOT NULL THEN w.w_egfr       ELSE 0 END) +
      (CASE WHEN n.bmd_score_100        IS NOT NULL THEN w.w_bmd        ELSE 0 END) +
      (CASE WHEN n.truage_score_100     IS NOT NULL THEN w.w_truage     ELSE 0 END) +
      (CASE WHEN n.small_hdl_score_100  IS NOT NULL THEN w.w_small_hdl  ELSE 0 END) +
      (CASE WHEN n.rem_score_100        IS NOT NULL THEN w.w_rem        ELSE 0 END) +
      (CASE WHEN n.grip_score_100       IS NOT NULL THEN w.w_grip       ELSE 0 END) +
      (CASE WHEN n.swls_score_100       IS NOT NULL THEN w.w_swls       ELSE 0 END) +
      (CASE WHEN n.hei_score_100        IS NOT NULL THEN w.w_hei        ELSE 0 END)
    ) AS present_weight_sum,

    -- Weighted sum of normalized scores
    (
      COALESCE(n.ogtt_score_100,0)      * w.w_ogtt      +
      COALESCE(n.apob_score_100,0)      * w.w_apob      +
      COALESCE(n.vo2_score_100,0)       * w.w_vo2       +
      COALESCE(n.crp_score_100,0)       * w.w_crp       +
      COALESCE(n.bmi_score_100,0)       * w.w_bmi       +
      COALESCE(n.packyears_score_100,0) * w.w_packyears +
      COALESCE(n.moca_score_100,0)      * w.w_moca      +
      COALESCE(n.mvpa_score_100,0)      * w.w_mvpa      +
      COALESCE(n.cac_score_100,0)       * w.w_cac       +
      COALESCE(n.hrv_score_100,0)       * w.w_hrv       +
      COALESCE(n.phq9_score_100,0)      * w.w_phq9      +
      COALESCE(n.alt_score_100,0)       * w.w_alt       +
      COALESCE(n.egfr_score_100,0)      * w.w_egfr      +
      COALESCE(n.bmd_score_100,0)       * w.w_bmd       +
      COALESCE(n.truage_score_100,0)    * w.w_truage    +
      COALESCE(n.small_hdl_score_100,0) * w.w_small_hdl +
      COALESCE(n.rem_score_100,0)       * w.w_rem       +
      COALESCE(n.grip_score_100,0)      * w.w_grip      +
      COALESCE(n.swls_score_100,0)      * w.w_swls      +
      COALESCE(n.hei_score_100,0)       * w.w_hei
    ) AS weighted_sum_scores

  FROM normalized n
  CROSS JOIN weights w
)

SELECT DISTINCT
  s.data_timestamp AS snapshot_date,
  s.user_id,
  s.full_name,
  s.username,
  s.sex,
  s.age_years,

  -- Raw metric values for transparency
  s.ogtt_2h_mgdl, s.apob_mgdl, s.vo2_max, s.crp_mgl, s.bmi,
  s.pack_years, s.moca_score, s.mvpa_weekly_min, s.cac, s.hrv_rmssd,
  s.phq9_score, s.alt_uL, s.egfr, s.bmd_tscore, s.inbody_bmc,
  s.biological_age, s.truage_delta_years, s.small_hdl, s.rem_pct,
  s.grip_kg, s.swls_score, s.hei_score,

  -- Normalized 0–100 scores
  s.ogtt_score_100, s.apob_score_100, s.vo2_score_100, s.crp_score_100,
  s.bmi_score_100, s.packyears_score_100, s.moca_score_100, s.mvpa_score_100,
  s.cac_score_100, s.hrv_score_100, s.phq9_score_100, s.alt_score_100,
  s.egfr_score_100, s.bmd_score_100, s.truage_score_100, s.small_hdl_score_100,
  s.rem_score_100, s.grip_score_100, s.swls_score_100, s.hei_score_100,

  -- Weighting context
  s.present_weight_sum,

  -- Weighted normalized score (0–100)
  CASE
    WHEN s.present_weight_sum > 0
      THEN ROUND( (s.weighted_sum_scores / s.present_weight_sum)::numeric, 1)
    ELSE NULL
  END AS weighted_normalized_score_0_100,

  -- Final Longevity Score (300–950)
  CASE
    WHEN s.present_weight_sum > 0
      THEN ROUND( 300 + ((s.weighted_sum_scores / s.present_weight_sum) / 100.0) * 650.0, 0)
    ELSE NULL
  END AS longevity_score_300_950

FROM scored s
WHERE 1=1
--AND s.data_timestamp >= '2025-01-01' 
--AND s.user_id = '17585'
ORDER BY s.full_name;