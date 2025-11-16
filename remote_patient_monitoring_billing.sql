{% set from_dttm = "'" + from_dttm + "'" if from_dttm is defined else "'2025-07-01'" %}
{% set to_dttm = "'" + to_dttm + "'" if to_dttm is defined else "'2027-01-01'" %}
{% set filter_value = "'" + "', '".join(filter_values('tag')) + "'" %}

SELECT 
  UnionQuery.*,
  CASE WHEN UnionQuery.event_name IS NULL THEN 'None' ELSE UnionQuery.event_name END AS namestring,
  org_members_view.full_name,
  org_members_view.email,
  org_members_view.user_uuid,
  CASE WHEN UnionQuery.type_99457 IS NULL THEN 'No Data' ELSE 'Has Data' END AS show_all_users,
  {{ from_dttm }}::date AS filter_date_start,
  {{ to_dttm }}::date   AS filter_date_end

FROM public.org_members_view

LEFT JOIN (
  SELECT
    rpm_dashboard_items.type_99457,
    rpm_dashboard_items.event_name,
    rpm_dashboard_items.time_log_type,
    rpm_dashboard_items.created_at AS time_begin,
    rpm_dashboard_items.time_end,
    org_members_view.user_uuid AS user_uuid_sq,
    CASE
      WHEN rpm_dashboard_items.seconds_elapsed > 600 THEN 600
      ELSE rpm_dashboard_items.seconds_elapsed
    END AS seconds,
    pro_user.full_name AS practitioner_name,
    pro_user.email AS practitioner_email,
    rpm_dashboard_items.month,
    rpm_dashboard_items.date,
    NULL AS notes

  FROM public.org_members_view
  INNER JOIN public.rpm_dashboard_items 
    ON rpm_dashboard_items.shared_uuid = org_members_view.user_uuid
    AND type_99457 IS NOT NULL
    AND rpm_dashboard_items.date >= {{ from_dttm }}::date
    AND rpm_dashboard_items.date <= {{ to_dttm }}::date
  LEFT JOIN public.org_members_view AS pro_user
    ON rpm_dashboard_items.uuid = pro_user.user_uuid
  WHERE 1=1

  UNION

  SELECT 
    'Manual Time Log' AS type_99457,
    time_type AS event_name,
    time_type AS time_log_type,
    start_at AS time_begin,
    end_at AS time_end,
    org_members_view.user_uuid AS user_uuid_sq,
    EXTRACT(EPOCH FROM end_at - start_at) AS seconds,
    pro_user.full_name AS practitioner_name,
    pro_user.email AS practitioner_email,
    TO_CHAR(start_at, 'YYYY-MM') AS month,
    DATE_TRUNC('day', start_at) AS date,
    notes

  FROM public.org_members_view
  INNER JOIN time_logs
    ON org_members_view.user_id = time_logs.user_id
    AND start_at >= {{ from_dttm }}::date
    AND start_at <= {{ to_dttm }}::date
  LEFT JOIN public.org_members_view AS pro_user
    ON time_logs.practitioner_uuid = pro_user.user_uuid
  WHERE 1=1
) AS UnionQuery
ON org_members_view.user_uuid = UnionQuery.user_uuid_sq

WHERE 1=1
  AND {{ active_org_clients() }}
  --AND {{ active_org_clients(org_uuid = 'b64dec9e-5921-4e1e-89af-c365c3084d7d') }}

  {% if filter_value and filter_value|length > 2 %}
    AND org_members_view.entity_tags ? ANY(ARRAY[{{ filter_value }}]::text[])
  {% endif %}
