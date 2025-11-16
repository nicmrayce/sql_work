WITH paying_plans AS (
    SELECT DISTINCT
        organizations.id AS org_id,
        CONCAT(organizations.name,' [',organizations.id,']') AS org_name,
        payola_subscriptions.created_at AS subscription_created,
         CASE
          WHEN canceled_at IS NOT NULL THEN canceled_at
          WHEN payola_subscriptions.current_period_end >= current_date THEN NULL
          WHEN payola_subscriptions.current_period_end < current_date THEN payola_subscriptions.current_period_end
          WHEN payola_subscriptions.canceled_at IS NULL THEN payola_subscriptions.ended_at
          WHEN payola_subscriptions.state = 'canceled' AND payola_subscriptions.current_period_end >= current_date THEN '2019-01-01'
          ELSE payola_subscriptions.canceled_at
        END AS canceled_at,
        plan.name AS plan_name,
        payola_subscriptions.amount AS amount
    FROM public.organizations
    LEFT JOIN payola_subscriptions ON organizations.id = payola_subscriptions.owner_id
        AND payola_subscriptions.owner_type = 'Organization'
        AND payola_subscriptions.plan_id <> 28
        AND payola_subscriptions.state NOT IN ('errored')
    LEFT JOIN subscription_plans AS plan ON payola_subscriptions.plan_id = plan.id
    WHERE (payola_subscriptions.amount > 0
       OR plan.name = 'Pro (Unlimited Clients) Paying Externally')
), subscription_periods AS (
    SELECT
        org_id,
        DATE_TRUNC('month', subscription_created) AS sub_month,
        DATE_TRUNC('month', canceled_at) AS cancel_month
    FROM paying_plans
), active_months AS (
    SELECT
        sp.org_id,
        generate_series(sp.sub_month, COALESCE(sp.cancel_month, date_trunc('month', now())), '1 month'::interval)::date AS month
    FROM subscription_periods sp
), monthly_active_orgs AS (
    SELECT
        am.month,
        COUNT(DISTINCT am.org_id) AS active_orgs
    FROM active_months am
    GROUP BY am.month
), new_orgs_per_month AS (
    SELECT
        sp.sub_month AS month,
        COUNT(DISTINCT sp.org_id) AS new_orgs
    FROM subscription_periods sp
    GROUP BY sp.sub_month
), canceled_orgs_per_month AS (
    SELECT
        sp.cancel_month AS month,
        COUNT(DISTINCT sp.org_id) AS canceled_orgs
    FROM subscription_periods sp
    GROUP BY sp.cancel_month
), months AS (
    SELECT generate_series(
        (SELECT MIN(sub_month) FROM subscription_periods),
        date_trunc('month', now()),
        '1 month'::interval
    )::date AS month
), cumulative_net_new AS (
    SELECT
        m.month,
        COALESCE(mao.active_orgs, 0) AS active_orgs,
        COALESCE(nom.new_orgs, 0) AS new_orgs,
        COALESCE(com.canceled_orgs, 0) AS canceled_orgs
    FROM months m
    LEFT JOIN monthly_active_orgs mao ON m.month = mao.month
    LEFT JOIN new_orgs_per_month nom ON m.month = nom.month
    LEFT JOIN canceled_orgs_per_month com ON m.month = com.month
)
SELECT
    month,
    active_orgs AS cumulative_orgs,
    new_orgs,
    canceled_orgs,
    new_orgs - canceled_orgs AS net_change
FROM cumulative_net_new
