{% set filter_value = "'" + "', '".join(filter_values('tag')) + "'"  %}

WITH program_data AS (
    SELECT 
        org_members_view.user_id, 
        COALESCE(
            (profiles.entity_attributes -> 'profile_attributes' ->> 'Program Start Date')::date, 
            organization_memberships.created_at
        ) AS program_start_date,  
        '20-day' AS program_length  
    FROM 
        org_members_view
    LEFT JOIN 
        profiles ON profiles.user_id = org_members_view.user_id
    LEFT JOIN 
        organization_memberships ON organization_memberships.user_id = org_members_view.user_id
    WHERE 
        org_members_view.accepted = TRUE
        AND (
            (profiles.entity_attributes -> 'profile_attributes' ->> 'Program Start Date')::date IS NOT NULL
            OR organization_memberships.created_at IS NOT NULL
        )
        AND {{ active_org_clients() }}
        --AND org_members_view.organization_uuid = '3004eb2e-b1b7-4d96-8da9-fae7fa7605dd'
),
actual_start_weights AS (
    SELECT 
        program_data.user_id,
        MIN(weights.timestamp) AS start_weight_date,
        (ARRAY_AGG(
            CASE 
                WHEN weights.unit = 'kg' THEN weights.value * 2.20462  -- Convert kg to lbs
                ELSE weights.value
            END
            ORDER BY weights.timestamp
        ))[1] AS actual_start_weight
    FROM 
        program_data
    JOIN 
        weights ON program_data.user_id = weights.user_id
    WHERE 
        weights.timestamp >= program_data.program_start_date
    GROUP BY 
        program_data.user_id
),
daily_latest_weights AS (
    SELECT 
        program_data.user_id,
        DATE(weights.timestamp) AS weight_date,
        MAX(weights.timestamp) AS latest_timestamp
    FROM 
        program_data
    JOIN 
        weights ON program_data.user_id = weights.user_id
    WHERE 
        weights.timestamp >= program_data.program_start_date
    GROUP BY 
        program_data.user_id, DATE(weights.timestamp)
),
latest_weight_readings AS (
    SELECT 
        daily_latest_weights.user_id,
        daily_latest_weights.weight_date,
        daily_latest_weights.latest_timestamp,
        CASE 
            WHEN weights.unit = 'kg' THEN weights.value * 2.20462
            ELSE weights.value
        END AS actual_weight
    FROM 
        daily_latest_weights
    JOIN 
        weights ON daily_latest_weights.user_id = weights.user_id
        AND daily_latest_weights.latest_timestamp = weights.timestamp
),
target_weights AS (
    SELECT 
        latest_weight_readings.user_id,
        latest_weight_readings.weight_date,
        latest_weight_readings.latest_timestamp,
        actual_start_weights.actual_start_weight,
        ROW_NUMBER() OVER (PARTITION BY latest_weight_readings.user_id ORDER BY latest_weight_readings.weight_date) AS days_since_start,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY latest_weight_readings.user_id ORDER BY latest_weight_readings.weight_date) BETWEEN 1 AND 20 THEN 
                actual_start_weights.actual_start_weight - (ROW_NUMBER() OVER (PARTITION BY latest_weight_readings.user_id ORDER BY latest_weight_readings.weight_date) * 0.5)
            WHEN ROW_NUMBER() OVER (PARTITION BY latest_weight_readings.user_id ORDER BY latest_weight_readings.weight_date) BETWEEN 21 AND 42 THEN 
                CASE 
                    WHEN ROW_NUMBER() OVER (PARTITION BY latest_weight_readings.user_id ORDER BY latest_weight_readings.weight_date) = 24 THEN 
                        actual_start_weights.actual_start_weight - (24 * 0.5)
                    ELSE 
                        actual_start_weights.actual_start_weight - (24 * 0.5) + 2
                END
            ELSE NULL
        END AS target_weight,
        latest_weight_readings.actual_weight
    FROM 
        latest_weight_readings
    JOIN 
        actual_start_weights ON latest_weight_readings.user_id = actual_start_weights.user_id
)
SELECT 
    org_members_view.full_name AS "Full Name", 
    org_members_view.time_zone, 
    target_weights.weight_date AS "Reading Date", 
    target_weights.actual_start_weight AS "Actual Start Weight",
    target_weights.actual_weight AS "Actual Weight", 
    target_weights.target_weight AS "Target Weight", 
    (target_weights.actual_weight - target_weights.actual_start_weight) AS "Weight Change (lbs)",
    target_weights.days_since_start AS "Days Since Start", 
    CASE 
        WHEN target_weights.actual_weight <= target_weights.target_weight THEN 'Yes' 
        ELSE 'No' 
    END AS "On Track",
    program_data.program_length AS "Program Length",   
    CASE 
        WHEN target_weights.days_since_start = 0 THEN 'Stage 1'
        WHEN target_weights.days_since_start BETWEEN 1 AND 20 THEN 'Stage 2'
        WHEN target_weights.days_since_start BETWEEN 21 AND 42 THEN 'Stage 3'
    END AS "Stage"
FROM 
    target_weights
JOIN 
    org_members_view ON target_weights.user_id = org_members_view.user_id
JOIN 
    program_data ON target_weights.user_id = program_data.user_id
WHERE 
    target_weights.days_since_start >= 1  -- Exclude Day 0
    AND target_weights.days_since_start <= 42
    AND {{ active_org_clients() }}
    --AND org_members_view.organization_uuid = '3004eb2e-b1b7-4d96-8da9-fae7fa7605dd'
ORDER BY 
    target_weights.weight_date DESC;
