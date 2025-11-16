{% set from_dttm = from_dttm if from_dttm is defined else '2025-01-01' %}
{% set to_dttm = to_dttm if to_dttm is defined else '2026-01-01' %}
{% set filter_value = "'" + "', '".join(filter_values('tag')) + "'" %}
WITH
-- 1: Insulin
insulin AS (
    SELECT 
        biomarkers.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN lab_report_results.value::float <= 5 THEN 0
            WHEN lab_report_results.value::float > 5 AND lab_report_results.value::float <= 7 THEN 1
            WHEN lab_report_results.value::float > 7 AND lab_report_results.value::float <= 10 THEN 2
            WHEN lab_report_results.value::float > 10 THEN 3
            ELSE 0
        END AS insulin_score
    FROM biomarkers
    INNER JOIN lab_report_results 
        ON biomarkers.uuid = lab_report_results.biomarker_uuid
    INNER JOIN org_members_view 
        ON biomarkers.user_id = org_members_view.user_id
    LEFT JOIN lab_components 
        ON lab_report_results.mapped_component_uuid = lab_components.uuid
    WHERE
        biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND COALESCE(
            lab_components.name, 
            lab_report_results.name, 
            lab_report_results.loinc_code_description, 
            lab_report_results.loinc_code
        ) ILIKE '%insulin%'
        AND COALESCE(
            lab_components.name, 
            lab_report_results.name, 
            lab_report_results.loinc_code_description, 
            lab_report_results.loinc_code
        ) NOT ILIKE '%pro%'
        AND COALESCE(
            lab_components.name, 
            lab_report_results.name, 
            lab_report_results.loinc_code_description, 
            lab_report_results.loinc_code
        ) NOT ILIKE '%igf%'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        AND biomarkers.result_date_time = (
            SELECT MAX(b2.result_date_time)
            FROM biomarkers b2
            INNER JOIN lab_report_results lrr2 
                ON b2.uuid = lrr2.biomarker_uuid
            LEFT JOIN lab_components lc2 
                ON lrr2.mapped_component_uuid = lc2.uuid
            WHERE 
                b2.user_id = biomarkers.user_id
                AND COALESCE(
                    lc2.name, 
                    lrr2.name, 
                    lrr2.loinc_code_description, 
                    lrr2.loinc_code
                ) ILIKE '%insulin%'
                AND COALESCE(
                    lc2.name, 
                    lrr2.name, 
                    lrr2.loinc_code_description, 
                    lrr2.loinc_code
                ) NOT ILIKE '%pro%'
                AND COALESCE(
                    lc2.name, 
                    lrr2.name, 
                    lrr2.loinc_code_description, 
                    lrr2.loinc_code
                ) NOT ILIKE '%igf%'
        )
)

,

-- 2: HbA1c
hba1c AS (
    SELECT 
        biomarkers.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN lab_report_results.value::float <= 5 THEN 0
            WHEN lab_report_results.value::float > 5 AND lab_report_results.value::float <= 5.7 THEN 1
            WHEN lab_report_results.value::float > 5.7 AND lab_report_results.value::float <= 6.4 THEN 2
            WHEN lab_report_results.value::float > 6.4 THEN 3
            ELSE 0
        END AS hba1c_score
    FROM biomarkers
    INNER JOIN lab_report_results 
        ON biomarkers.uuid = lab_report_results.biomarker_uuid
    INNER JOIN org_members_view 
        ON biomarkers.user_id = org_members_view.user_id
    LEFT JOIN lab_components 
        ON lab_report_results.mapped_component_uuid = lab_components.uuid
    WHERE
        biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER 
        AND (
            COALESCE(
                lab_components.name, 
                lab_report_results.name, 
                lab_report_results.loinc_code_description, 
                lab_report_results.loinc_code
            ) ILIKE '%hba%' 
            OR COALESCE(
                lab_components.name, 
                lab_report_results.name, 
                lab_report_results.loinc_code_description, 
                lab_report_results.loinc_code
            ) ILIKE '%hba1c%'
        )
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        AND biomarkers.result_date_time = (
            SELECT MAX(b2.result_date_time)
            FROM biomarkers b2
            INNER JOIN lab_report_results lrr2 
                ON b2.uuid = lrr2.biomarker_uuid
            LEFT JOIN lab_components lc2 
                ON lrr2.mapped_component_uuid = lc2.uuid
            WHERE 
                b2.user_id = biomarkers.user_id
                AND b2.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER SUBQUERY
                AND (
                    COALESCE(
                        lc2.name, 
                        lrr2.name, 
                        lrr2.loinc_code_description, 
                        lrr2.loinc_code
                    ) ILIKE '%hba%' 
                    OR COALESCE(
                        lc2.name, 
                        lrr2.name, 
                        lrr2.loinc_code_description, 
                        lrr2.loinc_code
                    ) ILIKE '%hba1c%'
                )
        )
)
,
-- 3: Glucose Fasting
glucose_fasting AS (
    WITH lab1 AS (
        SELECT 
            org_members_view.user_id,
            org_members_view.full_name,
            org_members_view.email,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CAST(lab_report_results.value AS decimal) AS component_value
        FROM biomarkers
        INNER JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        INNER JOIN org_members_view
            ON biomarkers.user_id = org_members_view.user_id
        LEFT JOIN lab_components 
            ON lab_report_results.mapped_component_uuid = lab_components.uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND (lab_components.name = 'Glucose (Fasting)' OR lab_report_results.name = 'Glucose (Fasting)')
            AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    ),
    lab2 AS (
        SELECT 
            org_members_view.user_id,
            org_members_view.full_name,
            org_members_view.email,
            lab_result_components.result_date_time::timestamp AS result_date_time,
            CAST(lab_result_components.value AS decimal) AS component_value
        FROM lab_result_components
        INNER JOIN org_members_view
            ON lab_result_components.user_uuid::uuid = org_members_view.user_uuid
        WHERE
            lab_result_components.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND (lab_result_components.component_name = 'Glucose (Fasting)' OR lab_result_components.mapped_component_name = 'Glucose (Fasting)')
            AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    ),
    merged AS (
        SELECT * FROM lab1
        UNION ALL
        SELECT * FROM lab2
    )
    SELECT 
        m.user_id,
        m.full_name,
        m.email,
        CASE
            WHEN m.component_value <= 85 THEN 0
            WHEN m.component_value > 85 AND m.component_value <= 91 THEN 1
            WHEN m.component_value > 91 AND m.component_value <= 100 THEN 2
            WHEN m.component_value > 100 THEN 3
            ELSE 0
        END AS glucose_fasting_score
    FROM merged m
    WHERE m.result_date_time = (
        SELECT MAX(m2.result_date_time)
        FROM merged m2
        WHERE m2.user_id = m.user_id
    )
)
,
-- 4: Uric Acid
uric_acid AS (
    WITH ranked_uric AS (
        SELECT 
            org_members_view.user_id,
            org_members_view.full_name,
            org_members_view.email,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS component_value
        FROM org_members_view
        LEFT JOIN biomarkers
            ON org_members_view.user_id = biomarkers.user_id
        LEFT JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        LEFT JOIN lab_components
            ON lab_report_results.mapped_component_uuid = lab_components.uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER
            AND COALESCE(lab_components.name, lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) ILIKE '%uric%'
            AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    )
    SELECT 
        r.user_id,
        r.full_name,
        r.email,
        CASE
            WHEN r.component_value <= 5.0 THEN 0
            WHEN r.component_value > 5.0 AND r.component_value <= 5.9 THEN 1
            WHEN r.component_value > 5.9 AND r.component_value <= 8.0 THEN 2
            WHEN r.component_value > 8.0 THEN 3
            ELSE 0
        END AS uric_acid_score
    FROM ranked_uric r
    WHERE r.result_date_time = (
        SELECT MAX(r2.result_date_time)
        FROM ranked_uric r2
        WHERE r2.user_id = r.user_id
    )
)
,

-- 5: HOMA-IR
homa_ir AS (
    WITH insulin_cte AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS insulin_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER 
            AND lab_report_results.name ILIKE '%insulin%'
            AND lab_report_results.name NOT ILIKE '%igf%'
    ),
    glucose_cte AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS glucose_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER 
            AND lab_report_results.name ILIKE '%glucose%'
    ),
    joined_homa AS (
        SELECT 
            org_members_view.user_id,
            org_members_view.full_name,
            org_members_view.email,
            (g.glucose_value * i.insulin_value) / 405.0 AS homa_ir_value,
            GREATEST(i.result_date_time, g.result_date_time) AS latest_date
        FROM insulin_cte i
        INNER JOIN glucose_cte g 
            ON i.user_id = g.user_id
        INNER JOIN org_members_view 
            ON org_members_view.user_id = i.user_id
        WHERE i.insulin_value IS NOT NULL 
          AND g.glucose_value IS NOT NULL
          AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    )
    SELECT 
        j.user_id,
        j.full_name,
        j.email,
        CASE
            WHEN j.homa_ir_value <= 1.2 THEN 0
            WHEN j.homa_ir_value > 1.2 AND j.homa_ir_value <= 1.9 THEN 1
            WHEN j.homa_ir_value > 1.9 AND j.homa_ir_value <= 2.9 THEN 2
            WHEN j.homa_ir_value > 2.9 THEN 3
            ELSE 0
        END AS homa_ir_score
    FROM joined_homa j
    WHERE j.latest_date = (
        SELECT MAX(j2.latest_date)
        FROM joined_homa j2
        WHERE j2.user_id = j.user_id
    )
)

,
-- 6: Triglycerides
triglycerides AS (
    WITH ranked_trigly AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS component_value
        FROM biomarkers
        INNER JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- << PREFILTER 
            AND lab_report_results.name ILIKE '%triglyc%'
            AND lab_report_results.name NOT ILIKE '%ratio%'
    ),
    latest_trigly AS (
        SELECT 
            ranked_trigly.user_id,
            ranked_trigly.component_value,
            ranked_trigly.result_date_time
        FROM ranked_trigly
        INNER JOIN (
            SELECT 
                user_id,
                MAX(result_date_time) AS max_date
            FROM ranked_trigly
            GROUP BY user_id
        ) latest
        ON ranked_trigly.user_id = latest.user_id 
       AND ranked_trigly.result_date_time = latest.max_date
    )
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN latest_trigly.component_value <= 100 THEN 0
            WHEN latest_trigly.component_value > 100 AND latest_trigly.component_value <= 150 THEN 1
            WHEN latest_trigly.component_value > 150 AND latest_trigly.component_value <= 200 THEN 2
            WHEN latest_trigly.component_value > 200 THEN 3
            ELSE 0
        END AS triglycerides_score
    FROM latest_trigly
    INNER JOIN org_members_view
        ON org_members_view.user_id = latest_trigly.user_id
    WHERE 
        latest_trigly.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 7: HDL (Male)
hdl_male AS (
    WITH ranked_hdl AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS component_value
        FROM biomarkers
        INNER JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- PREFILTER 
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) = 'HDL Cholesterol'
    ),
    latest_hdl AS (
        SELECT 
            ranked_hdl.user_id,
            ranked_hdl.component_value,
            ranked_hdl.result_date_time
        FROM ranked_hdl
        INNER JOIN (
            SELECT 
                user_id,
                MAX(result_date_time) AS max_date
            FROM ranked_hdl
            GROUP BY user_id
        ) latest
        ON ranked_hdl.user_id = latest.user_id 
       AND ranked_hdl.result_date_time = latest.max_date
    )
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN latest_hdl.component_value >= 40 THEN 0
            WHEN latest_hdl.component_value > 35 AND latest_hdl.component_value < 40 THEN 1
            WHEN latest_hdl.component_value > 30 AND latest_hdl.component_value <= 35 THEN 2
            WHEN latest_hdl.component_value <= 30 THEN 3
            ELSE 0
        END AS hdl_male_score
    FROM latest_hdl
    INNER JOIN org_members_view
        ON org_members_view.user_id = latest_hdl.user_id
    WHERE 
        latest_hdl.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,

-- 8: HDL (Female)
hdl_female AS (
    WITH ranked_hdl AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS component_value
        FROM biomarkers
        INNER JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'  -- PREFILTER 
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) = 'HDL Cholesterol'
    ),
    latest_hdl AS (
        SELECT 
            ranked_hdl.user_id,
            ranked_hdl.component_value,
            ranked_hdl.result_date_time
        FROM ranked_hdl
        INNER JOIN (
            SELECT 
                user_id,
                MAX(result_date_time) AS max_date
            FROM ranked_hdl
            GROUP BY user_id
        ) latest
        ON ranked_hdl.user_id = latest.user_id 
       AND ranked_hdl.result_date_time = latest.max_date
    )
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN latest_hdl.component_value >= 50 THEN 0
            WHEN latest_hdl.component_value > 45 AND latest_hdl.component_value < 50 THEN 1
            WHEN latest_hdl.component_value > 40 AND latest_hdl.component_value <= 45 THEN 2
            WHEN latest_hdl.component_value <= 40 THEN 3
            ELSE 0
        END AS hdl_female_score
    FROM latest_hdl
    INNER JOIN org_members_view
        ON org_members_view.user_id = latest_hdl.user_id
    WHERE 
        latest_hdl.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 9: TG/HDL Ratio
-- 9: TG/HDL Ratio
tg_hdl_ratio AS (
    -- Latest explicit TG/HDL ratio
    WITH ratio_cte AS (
        SELECT DISTINCT ON (biomarkers.user_id)
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$' 
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS ratio_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}' -- PREFILTER
            AND lab_report_results.name ILIKE '%trig%' 
            AND lab_report_results.name ILIKE '%hdl%' 
            AND lab_report_results.name ILIKE '%ratio%'
        ORDER BY biomarkers.user_id, biomarkers.result_date_time DESC
    ),
    -- Latest Triglycerides
    trig_cte AS (
        SELECT DISTINCT ON (biomarkers.user_id)
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$' 
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS trig_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND lab_report_results.name ILIKE '%triglyc%'
            AND lab_report_results.name NOT ILIKE '%ratio%'
        ORDER BY biomarkers.user_id, biomarkers.result_date_time DESC
    ),
    -- Latest HDL
    hdl_cte AS (
        SELECT DISTINCT ON (biomarkers.user_id)
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$' 
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS hdl_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) = 'HDL Cholesterol'
        ORDER BY biomarkers.user_id, biomarkers.result_date_time DESC
    )
    -- Final Score
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) <= 1 THEN 0
            WHEN COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) > 1 AND COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) <= 2 THEN 1
            WHEN COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) > 2 AND COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) <= 3 THEN 2
            WHEN COALESCE(
                ratio_cte.ratio_value, 
                ROUND(trig_cte.trig_value / NULLIF(hdl_cte.hdl_value, 0), 2)
            ) > 3 THEN 3
            ELSE 0
        END AS tg_hdl_ratio_score
    FROM org_members_view
    LEFT JOIN ratio_cte 
        ON org_members_view.user_id = ratio_cte.user_id
    LEFT JOIN trig_cte 
        ON org_members_view.user_id = trig_cte.user_id
    LEFT JOIN hdl_cte 
        ON org_members_view.user_id = hdl_cte.user_id
    WHERE 
        COALESCE(ratio_cte.result_date_time, trig_cte.result_date_time, hdl_cte.result_date_time) 
        BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)

,
-- 10: TSH
-- 10: TSH
tsh AS (
    WITH tsh_ranked AS (
        SELECT 
            biomarkers.user_id,
            org_members_view.full_name,
            org_members_view.email,
            biomarkers.result_date_time::timestamp AS result_date_time,
            CASE 
              WHEN lab_report_results.value::text ~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
              THEN CAST(lab_report_results.value AS decimal)
              ELSE NULL
            END AS component_value
        FROM biomarkers
        INNER JOIN lab_report_results
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        INNER JOIN org_members_view
            ON biomarkers.user_id = org_members_view.user_id
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}' -- PREFILTER 
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) ILIKE '%tsh%'
            AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    ),
    latest_tsh AS (
        SELECT 
            tsh_ranked.user_id,
            tsh_ranked.full_name,
            tsh_ranked.email,
            tsh_ranked.component_value,
            tsh_ranked.result_date_time
        FROM tsh_ranked
        INNER JOIN (
            SELECT 
                user_id,
                MAX(result_date_time) AS max_date
            FROM tsh_ranked
            GROUP BY user_id
        ) latest
        ON tsh_ranked.user_id = latest.user_id 
       AND tsh_ranked.result_date_time = latest.max_date
    )
    SELECT 
        latest_tsh.user_id,
        latest_tsh.full_name,
        latest_tsh.email,
        CASE
            WHEN latest_tsh.component_value <= 2 THEN 0
            WHEN latest_tsh.component_value > 2 AND latest_tsh.component_value <= 3 THEN 1
            WHEN latest_tsh.component_value > 3 AND latest_tsh.component_value <= 4 THEN 2
            WHEN latest_tsh.component_value > 4 THEN 3
            ELSE 0
        END AS tsh_score
    FROM latest_tsh
    WHERE 
        latest_tsh.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)

,
-- 11: LDL/HDL Ratio
ldl_hdl_ratio AS (
    WITH ldl_ranked AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            lab_report_results.value::float AS ldl_value
        FROM biomarkers
        JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND lab_report_results.name ILIKE '%ldl%' 
            AND lab_report_results.name NOT ILIKE '%hdl%' 
            AND lab_report_results.name NOT ILIKE '%ratio%'
    ),
    hdl_ranked AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            lab_report_results.value::float AS hdl_value
        FROM biomarkers
        JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}' 
            AND lab_report_results.name ILIKE '%hdl%' 
            AND lab_report_results.name NOT ILIKE '%ldl%' 
            AND lab_report_results.name NOT ILIKE '%ratio%'
    ),
    joined_lipids AS (
        SELECT 
            ldl_ranked.user_id,
            ldl_ranked.result_date_time,
            ldl_ranked.ldl_value,
            hdl_ranked.hdl_value,
            ROUND((ldl_ranked.ldl_value / NULLIF(hdl_ranked.hdl_value, 0))::numeric, 2) AS ldl_hdl_ratio_value
        FROM ldl_ranked
        JOIN hdl_ranked
            ON ldl_ranked.user_id = hdl_ranked.user_id
           AND DATE(ldl_ranked.result_date_time) = DATE(hdl_ranked.result_date_time)
    ),
    latest_ldl_hdl AS (
        SELECT 
            joined_lipids.user_id,
            joined_lipids.ldl_hdl_ratio_value,
            joined_lipids.result_date_time
        FROM joined_lipids
        INNER JOIN (
            SELECT 
                user_id,
                MAX(result_date_time) AS max_date
            FROM joined_lipids
            GROUP BY user_id
        ) latest
        ON joined_lipids.user_id = latest.user_id 
       AND joined_lipids.result_date_time = latest.max_date
    )
    SELECT 
        latest_ldl_hdl.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN latest_ldl_hdl.ldl_hdl_ratio_value <= 3.5 THEN 0
            WHEN latest_ldl_hdl.ldl_hdl_ratio_value > 3.5 AND latest_ldl_hdl.ldl_hdl_ratio_value <= 4.0 THEN 1
            WHEN latest_ldl_hdl.ldl_hdl_ratio_value > 4.0 AND latest_ldl_hdl.ldl_hdl_ratio_value <= 4.5 THEN 2
            WHEN latest_ldl_hdl.ldl_hdl_ratio_value > 4.5 THEN 3
            ELSE 0
        END AS ldl_hdl_ratio_score
    FROM latest_ldl_hdl
    JOIN org_members_view 
        ON latest_ldl_hdl.user_id = org_members_view.user_id
    WHERE 
        latest_ldl_hdl.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 12: Total Cholesterol / HDL Ratio
chol_hdl_ratio AS (
    WITH hdl_ranked AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            lab_report_results.value::decimal AS hdl_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) ILIKE '%hdl%'
            AND lab_report_results.name NOT ILIKE '%ldl%'
            AND lab_report_results.name NOT ILIKE '%ratio%'
    ),
    chol_ranked AS (
        SELECT 
            biomarkers.user_id,
            biomarkers.result_date_time::timestamp AS result_date_time,
            lab_report_results.value::decimal AS total_cholesterol_value
        FROM biomarkers
        INNER JOIN lab_report_results 
            ON biomarkers.uuid = lab_report_results.biomarker_uuid
        WHERE 
            biomarkers.result_date_time BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
            AND COALESCE(lab_report_results.name, lab_report_results.loinc_code_description, lab_report_results.loinc_code) ILIKE '%cholesterol%'
            AND lab_report_results.name NOT ILIKE '%hdl%'
            AND lab_report_results.name NOT ILIKE '%ldl%'
            AND lab_report_results.name NOT ILIKE '%ratio%'
    ),
    joined_lipids AS (
        SELECT 
            hdl_ranked.user_id,
            hdl_ranked.result_date_time AS hdl_date,
            chol_ranked.result_date_time AS chol_date,
            hdl_ranked.hdl_value,
            chol_ranked.total_cholesterol_value,
            ROUND(chol_ranked.total_cholesterol_value / NULLIF(hdl_ranked.hdl_value, 0), 2) AS chol_hdl_ratio_value,
            GREATEST(hdl_ranked.result_date_time, chol_ranked.result_date_time) AS latest_date
        FROM hdl_ranked
        JOIN chol_ranked
            ON hdl_ranked.user_id = chol_ranked.user_id
    ),
    latest_joined AS (
        SELECT 
            joined_lipids.user_id,
            joined_lipids.chol_hdl_ratio_value,
            joined_lipids.latest_date
        FROM joined_lipids
        INNER JOIN (
            SELECT 
                user_id,
                MAX(GREATEST(hdl_date, chol_date)) AS max_date
            FROM joined_lipids
            GROUP BY user_id
        ) latest
        ON joined_lipids.user_id = latest.user_id 
       AND joined_lipids.latest_date = latest.max_date
    )
    SELECT 
        latest_joined.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN latest_joined.chol_hdl_ratio_value <= 4.0 THEN 0
            WHEN latest_joined.chol_hdl_ratio_value > 4.0 AND latest_joined.chol_hdl_ratio_value <= 4.5 THEN 1
            WHEN latest_joined.chol_hdl_ratio_value > 4.5 AND latest_joined.chol_hdl_ratio_value <= 5.0 THEN 2
            WHEN latest_joined.chol_hdl_ratio_value > 5.0 THEN 3
            ELSE 0
        END AS chol_hdl_ratio_score
    FROM latest_joined
    JOIN org_members_view 
        ON latest_joined.user_id = org_members_view.user_id
    WHERE 
        latest_joined.latest_date BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        AND {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 13: History
history AS (
    WITH history_ranked AS (
        SELECT 
            source_metrics.user_id,
            source_metrics.standard_value::numeric AS history_value,
            source_metrics.timestamp::timestamp AS recorded_at
        FROM source_metrics
        WHERE 
            source_metrics.data_source_url = 'patient_history_emi'
            AND source_metrics.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
    ),
    latest_history AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM history_ranked
        GROUP BY user_id
    )
    SELECT 
        history_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN history_ranked.history_value < 15 THEN 0
            WHEN history_ranked.history_value = 15 THEN 1
            WHEN history_ranked.history_value > 15 AND history_ranked.history_value <= 30 THEN 2
            WHEN history_ranked.history_value > 30 THEN 3
            ELSE 0
        END AS history_score
    FROM history_ranked
    JOIN latest_history 
        ON history_ranked.user_id = latest_history.user_id
       AND history_ranked.recorded_at = latest_history.latest_date
    JOIN org_members_view 
        ON history_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
),
-- 14: Family History
family_history AS (
    WITH family_history_ranked AS (
        SELECT 
            source_metrics.user_id,
            (source_metrics.metadata::json->>'family_history_of_diabetes') AS family_history_value,
            source_metrics.timestamp::timestamp AS recorded_at
        FROM source_metrics
        WHERE 
            source_metrics.data_source_url = 'patient_history_emi'
            AND source_metrics.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
    ),
    latest_family_history AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM family_history_ranked
        GROUP BY user_id
    )
    SELECT 
        family_history_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN family_history_ranked.family_history_value = 'Normal' THEN 0
            WHEN family_history_ranked.family_history_value = 'Obese' THEN 1
            WHEN family_history_ranked.family_history_value = 'Pre-Diabetes' THEN 2
            WHEN family_history_ranked.family_history_value = 'Diabetes' THEN 3
            ELSE 0
        END AS family_history_score
    FROM family_history_ranked
    JOIN latest_family_history 
        ON family_history_ranked.user_id = latest_family_history.user_id
       AND family_history_ranked.recorded_at = latest_family_history.latest_date
    JOIN org_members_view 
        ON family_history_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 15: Age
age AS (
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        MAX(
            CASE
                WHEN DATE_PART('year', AGE(NOW(), org_members_view.dob)) <= 18 THEN 0
                WHEN DATE_PART('year', AGE(NOW(), org_members_view.dob)) > 18 AND DATE_PART('year', AGE(NOW(), org_members_view.dob)) <= 40 THEN 1
                WHEN DATE_PART('year', AGE(NOW(), org_members_view.dob)) > 40 AND DATE_PART('year', AGE(NOW(), org_members_view.dob)) <= 60 THEN 2
                WHEN DATE_PART('year', AGE(NOW(), org_members_view.dob)) > 60 THEN 3
                ELSE 0
            END
        ) AS age_score
    FROM org_members_view
    WHERE {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
    {% if filter_values('tag')|length > 0 %}
    AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
    {% endif %}
    GROUP BY 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email
)



,
-- 16: Waist/Height
waist_height AS (
    WITH waist_height_ranked AS (
        SELECT 
            source_metrics.user_id,
            (source_metrics.metadata::json->>'waist_height_ratio')::numeric AS waist_height_value,
            source_metrics.timestamp::timestamp AS recorded_at
        FROM source_metrics
        WHERE 
            source_metrics.data_source_url = 'patient_history_emi'
            AND source_metrics.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
    ),
    latest_waist_height AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM waist_height_ranked
        GROUP BY user_id
    )
    SELECT 
        waist_height_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE 
            WHEN waist_height_ranked.waist_height_value <= 0.50 THEN 0
            WHEN waist_height_ranked.waist_height_value > 0.50 AND waist_height_ranked.waist_height_value <= 0.52 THEN 1
            WHEN waist_height_ranked.waist_height_value > 0.52 AND waist_height_ranked.waist_height_value <= 0.55 THEN 2
            WHEN waist_height_ranked.waist_height_value > 0.55 THEN 3
            ELSE 0
        END AS waist_height_score
    FROM waist_height_ranked
    JOIN latest_waist_height 
        ON waist_height_ranked.user_id = latest_waist_height.user_id
       AND waist_height_ranked.recorded_at = latest_waist_height.latest_date
    JOIN org_members_view 
        ON waist_height_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 17: Body Fat
body_fat AS (
    WITH body_fat_ranked AS (
        SELECT
            body_fats.user_id,
            body_fats.value::float AS body_fat_percentage,
            body_fats.timestamp AS recorded_at
        FROM public.body_fats
        WHERE 
            body_fats.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        ORDER BY body_fats.user_id, body_fats.timestamp DESC
    ),
    latest_body_fat AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM body_fat_ranked
        GROUP BY user_id
    )
    SELECT 
        body_fat_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE
            WHEN body_fat_ranked.body_fat_percentage <= 25 THEN 0
            WHEN body_fat_ranked.body_fat_percentage > 25 AND body_fat_ranked.body_fat_percentage <= 30 THEN 1
            WHEN body_fat_ranked.body_fat_percentage > 30 AND body_fat_ranked.body_fat_percentage <= 40 THEN 2
            WHEN body_fat_ranked.body_fat_percentage > 40 THEN 3
            ELSE 0
        END AS body_fat_score
    FROM body_fat_ranked
    JOIN latest_body_fat 
        ON body_fat_ranked.user_id = latest_body_fat.user_id
       AND body_fat_ranked.recorded_at = latest_body_fat.latest_date
    JOIN org_members_view 
        ON body_fat_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 18: BMI
bmi AS (
    WITH bmi_ranked AS (
        SELECT
            bmis.user_id,
            bmis.value::float AS bmi_value,
            bmis.timestamp AS recorded_at
        FROM public.bmis
        WHERE 
            bmis.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        ORDER BY bmis.user_id, bmis.timestamp DESC
    ),
    latest_bmi AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM bmi_ranked
        GROUP BY user_id
    )
    SELECT 
        bmi_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE 
            WHEN bmi_ranked.bmi_value <= 25 THEN 0
            WHEN bmi_ranked.bmi_value > 25 AND bmi_ranked.bmi_value <= 30 THEN 1
            WHEN bmi_ranked.bmi_value > 30 AND bmi_ranked.bmi_value <= 35 THEN 2
            WHEN bmi_ranked.bmi_value > 35 AND bmi_ranked.bmi_value <= 40 THEN 3
            ELSE 0
        END AS bmi_score
    FROM bmi_ranked
    JOIN latest_bmi 
        ON bmi_ranked.user_id = latest_bmi.user_id
       AND bmi_ranked.recorded_at = latest_bmi.latest_date
    JOIN org_members_view 
        ON bmi_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,
-- 19: Blood Pressure
blood_pressure AS (
    WITH bp_ranked AS (
        SELECT
            blood_pressures.user_id,
            blood_pressures.diastolic,
            blood_pressures.timestamp AS recorded_at
        FROM public.blood_pressures
        WHERE 
            blood_pressures.timestamp BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
        ORDER BY blood_pressures.user_id, blood_pressures.timestamp DESC
    ),
    latest_bp AS (
        SELECT 
            user_id,
            MAX(recorded_at) AS latest_date
        FROM bp_ranked
        GROUP BY user_id
    )
    SELECT 
        bp_ranked.user_id,
        org_members_view.full_name,
        org_members_view.email,
        CASE 
            WHEN bp_ranked.diastolic IS NULL OR bp_ranked.diastolic = 0 THEN 0
            WHEN bp_ranked.diastolic < 90 THEN 0
            WHEN bp_ranked.diastolic BETWEEN 90 AND 100 THEN 1
            WHEN bp_ranked.diastolic > 100 AND bp_ranked.diastolic <= 110 THEN 2
            WHEN bp_ranked.diastolic > 110 THEN 3
            ELSE 0
        END AS blood_pressure_score
    FROM bp_ranked
    JOIN latest_bp 
        ON bp_ranked.user_id = latest_bp.user_id
       AND bp_ranked.recorded_at = latest_bp.latest_date
    JOIN org_members_view 
        ON bp_ranked.user_id = org_members_view.user_id
    WHERE 
        {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
        {% if filter_values('tag')|length > 0 %}
        AND org_members_view.entity_tags ? ANY(ARRAY[{{ "'" + "', '".join(filter_values('tag')) + "'" }}]::text[])
        {% endif %}
)
,


combined AS (
    SELECT 
        org_members_view.user_id,
        org_members_view.full_name,
        org_members_view.email,
        COALESCE(insulin.insulin_score,0) AS insulin_score,
        COALESCE(hba1c.hba1c_score,0) AS hba1c_score,
        COALESCE(glucose_fasting.glucose_fasting_score,0) AS glucose_fasting_score,
        COALESCE(uric_acid.uric_acid_score,0) AS uric_acid_score,
        COALESCE(homa_ir.homa_ir_score,0) AS homa_ir_score,
        COALESCE(triglycerides.triglycerides_score,0) AS triglycerides_score,
        COALESCE(hdl_male.hdl_male_score,0) AS hdl_male_score,
        COALESCE(hdl_female.hdl_female_score,0) AS hdl_female_score,
        COALESCE(tg_hdl_ratio.tg_hdl_ratio_score,0) AS tg_hdl_ratio_score,
        COALESCE(tsh.tsh_score,0) AS tsh_score,
        COALESCE(ldl_hdl_ratio.ldl_hdl_ratio_score,0) AS ldl_hdl_ratio_score,
        COALESCE(chol_hdl_ratio.chol_hdl_ratio_score,0) AS chol_hdl_ratio_score,
        COALESCE(history.history_score,0) AS history_score,
        COALESCE(family_history.family_history_score,0) AS family_history_score,
        COALESCE(age.age_score,0) AS age_score,
        COALESCE(waist_height.waist_height_score,0) AS waist_height_score,
        COALESCE(body_fat.body_fat_score,0) AS body_fat_score,
        COALESCE(bmi.bmi_score,0) AS bmi_score,
        COALESCE(blood_pressure.blood_pressure_score,0) AS blood_pressure_score

        -- Add all other metrics here as columns
    FROM org_members_view
    LEFT JOIN insulin ON insulin.user_id = org_members_view.user_id
    LEFT JOIN hba1c ON hba1c.user_id = org_members_view.user_id
    LEFT JOIN glucose_fasting ON glucose_fasting.user_id = org_members_view.user_id
    LEFT JOIN uric_acid ON uric_acid.user_id = org_members_view.user_id
    LEFT JOIN homa_ir ON homa_ir.user_id = org_members_view.user_id
    LEFT JOIN triglycerides ON triglycerides.user_id = org_members_view.user_id
    LEFT JOIN hdl_male ON hdl_male.user_id = org_members_view.user_id
    LEFT JOIN hdl_female ON hdl_female.user_id = org_members_view.user_id
    LEFT JOIN tg_hdl_ratio ON tg_hdl_ratio.user_id = org_members_view.user_id
    LEFT JOIN tsh ON tsh.user_id = org_members_view.user_id
    LEFT JOIN ldl_hdl_ratio ON ldl_hdl_ratio.user_id = org_members_view.user_id
    LEFT JOIN chol_hdl_ratio ON chol_hdl_ratio.user_id = org_members_view.user_id
    LEFT JOIN history ON history.user_id = org_members_view.user_id
    LEFT JOIN family_history ON family_history.user_id = org_members_view.user_id
    LEFT JOIN age ON age.user_id = org_members_view.user_id
    LEFT JOIN waist_height ON waist_height.user_id = org_members_view.user_id
    LEFT JOIN body_fat ON body_fat.user_id = org_members_view.user_id
    LEFT JOIN bmi ON bmi.user_id = org_members_view.user_id
    LEFT JOIN blood_pressure ON blood_pressure.user_id = org_members_view.user_id
    -- LEFT JOIN other_metric ON other_metric.user_id = omv.user_id
    WHERE {{ active_org_clients(org_uuid='18bb5459-2ebe-4b55-9bbe-9cc9cf35d348') }}
)

SELECT
    user_id,
    full_name,
    email,
    insulin_score,
    hba1c_score,
    glucose_fasting_score,
    uric_acid_score,
    homa_ir_score,
    triglycerides_score,
    hdl_male_score,
    hdl_female_score,
    tg_hdl_ratio_score,
    tsh_score,
    ldl_hdl_ratio_score,
    chol_hdl_ratio_score,
    history_score,
    family_history_score,
    age_score,
    waist_height_score,
    body_fat_score,
    bmi_score,
    blood_pressure_score,
    -- other metric columns...
    (insulin_score 
        + hba1c_score 
        + glucose_fasting_score 
        + uric_acid_score
        + homa_ir_score 
        + triglycerides_score
        + hdl_male_score
        + hdl_female_score
        + tg_hdl_ratio_score
        + tsh_score
        + ldl_hdl_ratio_score
        + chol_hdl_ratio_score
        + history_score
        + family_history_score
        + age_score
        + waist_height_score
        + body_fat_score
        + bmi_score
        + blood_pressure_score) AS IRS_total_score
FROM combined
ORDER BY IRS_total_score DESC;