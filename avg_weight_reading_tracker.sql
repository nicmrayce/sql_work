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
initial_weights AS (
    SELECT 
        program_data.user_id, 
        MIN(weights.timestamp) AS initial_date,  
        MIN(
            CASE 
                WHEN weights.unit = 'kg' THEN weights.value * 2.20462  -- Convert kg to lbs
                ELSE weights.value  -- Assume already in lbs
            END
        ) AS initial_weight     
    FROM 
        program_data
    JOIN 
        weights ON program_data.user_id = weights.user_id
    WHERE 
        weights.timestamp >= program_data.program_start_date
    GROUP BY 
        program_data.user_id
),
target_weights AS (
    SELECT 
        initial_weights.user_id, 
        initial_weights.initial_date, 
        initial_weights.initial_weight, 
        EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) AS days_since_start,  
        CASE 
            WHEN EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) = 0 THEN initial_weights.initial_weight  
            WHEN EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) BETWEEN 1 AND 20 THEN initial_weights.initial_weight - (EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) / 2)
            WHEN EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) = 24 THEN initial_weights.initial_weight - 12 -- Day 24 Rule
            WHEN EXTRACT(DAY FROM weights.timestamp - initial_weights.initial_date) BETWEEN 21 AND 42 THEN 
                initial_weights.initial_weight - 12  -- Stage 3 maintains weight of Day 24 (+/- 2 lbs)
            ELSE NULL  
        END AS target_weight, 
        weights.timestamp AS weight_date, 
        CASE 
            WHEN weights.unit = 'kg' THEN weights.value * 2.20462  -- Convert kg to lbs
            ELSE weights.value  -- Assume already in lbs
        END AS actual_weight
    FROM 
        initial_weights
    JOIN 
        weights ON initial_weights.user_id = weights.user_id
    JOIN 
        program_data ON initial_weights.user_id = program_data.user_id
    WHERE 
        weights.timestamp >= initial_weights.initial_date
)
SELECT DISTINCT ON (target_weights.user_id) -- Fetch only the most recent reading per user
    org_members_view.full_name AS "Full Name", 
    org_members_view.time_zone, 
    target_weights.weight_date AS "Most Recent Reading", 
    target_weights.initial_weight AS "Starting Weight", 
    target_weights.actual_weight AS "Actual Weight", 
    target_weights.target_weight AS "Target Weight", 
    (target_weights.actual_weight - target_weights.initial_weight) AS "Weight Change (lbs)", 
    target_weights.days_since_start AS "Days Since Start", 
    CASE 
        WHEN target_weights.actual_weight <= target_weights.target_weight + 2 AND target_weights.actual_weight >= target_weights.target_weight - 2 THEN 'Yes' 
        ELSE 'No' 
    END AS "On Track",
    program_data.program_length AS "Program Length",   
    CASE 
        WHEN target_weights.days_since_start = 0 THEN 'Stage 1'
        WHEN target_weights.days_since_start BETWEEN 1 AND 20 THEN 'Stage 2'
        WHEN target_weights.days_since_start BETWEEN 21 AND 42 THEN 'Stage 3'
    END AS "Current Stage"
FROM 
    target_weights
JOIN 
    org_members_view ON target_weights.user_id = org_members_view.user_id
JOIN 
    program_data ON target_weights.user_id = program_data.user_id
WHERE
    1=1
    AND target_weights.days_since_start >= 1  -- Exclude Day 0
    AND target_weights.days_since_start <= 42
    AND {{ active_org_clients() }}
    --AND org_members_view.organization_uuid = '3004eb2e-b1b7-4d96-8da9-fae7fa7605dd'
  {% if filter_value|length > 2 %}
    AND public.org_members_view.entity_tags ? ANY(ARRAY[{{ filter_value }}]::text[])
  {% endif %}
ORDER BY 
    target_weights.user_id, target_weights.weight_date DESC;
