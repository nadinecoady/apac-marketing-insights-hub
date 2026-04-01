-- ============================================================
-- APAC SALES CALL THEME ANALYSIS — Rolling 90 Days
-- Covers: ANZ | Japan | Greater China Region | Rest of Asia
-- Includes: Plus Acquisition calls breakout
--
-- HOW TO USE:
--   Run as-is at any time — dates auto-calculate to the last 90 days.
--   To run for a specific window, replace the DECLARE lines with:
--     DECLARE analysis_start DATE DEFAULT '2025-XX-XX';
--     DECLARE analysis_end   DATE DEFAULT '2025-XX-XX';
--
-- REQUIREMENTS:
--   Run via data-portal MCP: query_bigquery
--   For summaries/pivots after: analyze_query_results
--
-- NOTES ON APAC FILTERS:
--   - sur.region = 'APAC' filters to APAC sales user roles
--   - sa.account_subregion provides sub-region bucketing:
--       ANZ, Japan, Greater China, SEA, India, Rest of APAC
--   - "Plus Acquisition" = primary_product_interest = 'Plus' AND motion = 'Acquisition'
--
-- SCHEMA NOTES (verified March 2026):
--   - sales_calls: event_id, event_start, call_duration_minutes, has_transcript,
--     transcript_details (ARRAY<STRUCT<..., full_transcript ARRAY<STRUCT<
--       transcript_block_start, sequence_number, speaker_name, speaker_email, speaker_text>>>>),
--     attendee_details (ARRAY<STRUCT<attendee_email, response_status, is_organizer, is_shopify_employee>>),
--     most_recent_salesforce_opportunity_id, call_sentiment, call_disposition
--   - sales_opportunities: opportunity_id, salesforce_account_id (→ sa.account_id),
--     salesforce_owner_id (→ u.user_id), current_stage_name, primary_product_interest
--   - sales_accounts: account_id, account_region, account_subregion, region
--   - sales_users_daily_snapshot: date (partition), user_id, user_role
--   - sales_user_roles: user_role, segment, region, subregion, motion
-- ============================================================

DECLARE analysis_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
DECLARE analysis_end   DATE DEFAULT CURRENT_DATE();

-- ============================================================
-- STEP 1: Isolate prospect voice, enrich with APAC sub-region
-- ============================================================
WITH prospect_sentences AS (
    SELECT
        sc.event_id,
        DATE(sc.event_start) AS call_date,
        sc.call_sentiment,
        sc.call_disposition,
        sur.segment    AS rep_segment,
        sur.region     AS rep_region,
        sur.motion     AS rep_motion,

        -- Sub-region bucketing using account_subregion
        -- sa.account_subregion values: ANZ, Japan, Greater China, SEA, India, Rest of APAC
        CASE
            WHEN sa.account_subregion = 'ANZ' THEN 'ANZ'
            WHEN sa.account_subregion = 'Japan' THEN 'Japan'
            WHEN sa.account_subregion = 'Greater China' THEN 'Greater China Region'
            WHEN sa.account_subregion IN ('SEA', 'India', 'Rest of APAC')
                THEN 'Rest of Asia'
            ELSE 'Other / Unclassified'
        END AS apac_subregion,

        -- Segment bucketing
        -- sur.segment values: Large, Enterprise, Unicorn, Large Mid-Mkt, Mid-Mkt, Mid-Mkt SMB, SMB, Not Applicable
        CASE
            WHEN sur.segment IN ('Large', 'Enterprise', 'Unicorn') THEN 'large_accounts'
            WHEN sur.segment IN ('Large Mid-Mkt', 'Mid-Mkt') THEN 'mid_market'
            WHEN sur.segment IN ('Mid-Mkt SMB', 'SMB') THEN 'smb'
            ELSE 'other'
        END AS segment_bucket,

        so.current_stage_name,
        so.primary_product_interest,

        -- Plus Acquisition flag (breakout column)
        CASE
            WHEN so.primary_product_interest = 'Plus'
             AND sur.motion = 'Acquisition'
            THEN TRUE ELSE FALSE
        END AS is_plus_acquisition,

        sentence.speaker_text

    FROM `shopify-dw.sales.sales_calls` sc
    JOIN `shopify-dw.sales.sales_opportunities` so
        ON sc.most_recent_salesforce_opportunity_id = so.opportunity_id
    JOIN `shopify-dw.sales.sales_accounts` sa
        ON so.salesforce_account_id = sa.account_id
    JOIN `shopify-dw.sales.sales_users_daily_snapshot` u
        ON so.salesforce_owner_id = u.user_id
        AND u.date = DATE(sc.event_start)
    JOIN `shopify-dw.sales.sales_user_roles` sur
        ON u.user_role = sur.user_role,
    UNNEST(sc.transcript_details) AS transcript,
    UNNEST(transcript.full_transcript) AS sentence

    WHERE sc.has_transcript = TRUE
        AND ARRAY_LENGTH(sc.transcript_details) > 0
        AND DATE(sc.event_start) >= analysis_start
        AND DATE(sc.event_start) < analysis_end
        AND u.date BETWEEN analysis_start AND analysis_end
        AND sc.call_duration_minutes >= 5
        AND LENGTH(sentence.speaker_text) >= 30
        AND sur.region = 'APAC'

        -- Prospect voice only (exclude Shopify employees)
        AND sentence.speaker_email NOT IN (
            SELECT attendee.attendee_email
            FROM UNNEST(sc.attendee_details) AS attendee
            WHERE attendee.is_shopify_employee = TRUE
        )
),

-- ============================================================
-- STEP 2: Detect themes per call
-- ============================================================
call_themes AS (
    SELECT
        event_id,
        call_date,
        call_sentiment,
        call_disposition,
        rep_segment,
        apac_subregion,
        segment_bucket,
        current_stage_name,
        primary_product_interest,
        is_plus_acquisition,

        -- ── OBJECTIONS & FRICTION ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:cost|price|pricing|budget|tco|total cost|expensive|afford|spend|invest|fee|subscription|contract value)')
            THEN 1 ELSE 0 END) AS theme_cost_tco,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:procurement|legal|contract|vendor|rfp|rfi|security questionnaire|compliance|soc|pci|gdpr|terms|approval process|sign.?off)')
            THEN 1 ELSE 0 END) AS theme_procurement_process,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:risk|afraid|fear|concern|disrupt|break|lose|downtime|rollback|revert|not sure|worried|hesit)')
            THEN 1 ELSE 0 END) AS theme_risk_concern,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:stakeholder|buying committee|decision.?maker|executive|board|c.suite|approval|champion|internal buy.?in)')
            THEN 1 ELSE 0 END) AS theme_internal_stakeholders,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:security|compliance|soc.?2|pci|gdpr|hipaa|privacy|data protection|encrypt|breach|audit)')
            THEN 1 ELSE 0 END) AS theme_security_compliance,

        -- ── QUESTIONS & EVALUATION CRITERIA ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:timeline|go.live|launch date|implementation time|how (?:fast|long|quickly)|migrate.*(?:month|week|day)|deadline|time to (?:market|value|launch))')
            THEN 1 ELSE 0 END) AS theme_timeline,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:erp|sap|netsuite|oracle|integration|api|middleware|connect|data.?flow|tech.?stack|ecosystem|third.?party)')
            THEN 1 ELSE 0 END) AS theme_integration_tech,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:professional service|implementation support|partner|onboard|training|managed|agency|consultant|systems integrator|si partner)')
            THEN 1 ELSE 0 END) AS theme_implementation_services,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:custom|headless|composable|hydrogen|storefront api|commerce components|flexible|bespoke|build vs buy)')
            THEN 1 ELSE 0 END) AS theme_customization,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:scale|scalab|performance|speed|uptime|traffic|load|bfcm|black friday|peak|high volume)')
            THEN 1 ELSE 0 END) AS theme_scalability,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:analytics|reporting|dashboard|data|insight|attribution|measure|metric|kpi|visibility)')
            THEN 1 ELSE 0 END) AS theme_analytics_reporting,

        -- ── COMPETITIVE & MIGRATION ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:compet|magento|salesforce commerce|sfcc|bigcommerce|woocommerce|adobe commerce|compare|alternative|switch from|currently on|currently using)')
            THEN 1 ELSE 0 END) AS theme_competitive,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:migrat|replatform|switch|move from|move over|transition|cut.?over|lift and shift)')
            THEN 1 ELSE 0 END) AS theme_migration,

        -- ── PRODUCT-SPECIFIC ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:checkout|conversion|shop.?pay|cart|payment|one.?click|buy.?button|express checkout)')
            THEN 1 ELSE 0 END) AS theme_checkout_payments,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:pos |point.of.sale|retail|in.store|omnichannel|brick.and.mortar|physical store|unified commerce|offline)')
            THEN 1 ELSE 0 END) AS theme_pos_omnichannel,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:b2b|wholesale|bulk|dealer|distributor|trade|net.?(?:30|60|90)|purchase order|invoice|account.?based pricing)')
            THEN 1 ELSE 0 END) AS theme_b2b,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:international|multi.market|cross.border|currency|localization|translation|region|local payment|local language)')
            THEN 1 ELSE 0 END) AS theme_international,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:\bai\b|artificial intelligence|machine learning|automat|sidekick|copilot|agentic|chatgpt|generative)')
            THEN 1 ELSE 0 END) AS theme_ai,

        -- ── GROWTH GOALS ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:grow|revenue|sales volume|conversion rate|aov|average order|expand|market share|new market|increase)')
            THEN 1 ELSE 0 END) AS theme_growth,

        -- ── APAC-SPECIFIC THEMES ──
        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:local payment|alipay|wechat pay|paynow|grab pay|gcash|local acquir|payment method|local gateway)')
            THEN 1 ELSE 0 END) AS theme_local_payments,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:chinese|mandarin|japanese|korean|bahasa|local language|localiz|translat|language support|multi.?lingual)')
            THEN 1 ELSE 0 END) AS theme_localisation,

        SUM(CASE WHEN REGEXP_CONTAINS(LOWER(speaker_text),
            r'(?:cross.border|cbec|daigou|tmall|lazada|shopee|tokopedia|rakuten|marketplace|platform fee|multi.channel)')
            THEN 1 ELSE 0 END) AS theme_marketplace_crossborder

    FROM prospect_sentences
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

-- ============================================================
-- STEP 3: Stage grouping helper
-- ============================================================
call_themes_staged AS (
    SELECT
        *,
        CASE
            WHEN current_stage_name IN ('Pre-Qualified', 'Demonstrate', 'Solution') THEN 'Early-Stage'
            WHEN current_stage_name IN ('Envision', 'Deal Craft')                  THEN 'Late-Stage'
            WHEN current_stage_name = 'Closed Won'                                 THEN 'Closed Won'
            WHEN current_stage_name = 'Closed Lost'                                THEN 'Closed Lost'
            ELSE 'Other'
        END AS stage_group
    FROM call_themes
)

-- ============================================================
-- FINAL OUTPUT: Theme prevalence by APAC sub-region, outcome, and segment
-- Shows: % of calls where prospect mentioned each theme (1+ = any mention)
-- ============================================================
SELECT
    apac_subregion,
    stage_group,
    segment_bucket,

    COUNT(DISTINCT event_id)    AS total_calls,
    DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS window_start,
    CURRENT_DATE()              AS window_end,

    -- ── OBJECTIONS ────────────────────────────────────────────
    ROUND(SAFE_DIVIDE(COUNTIF(theme_cost_tco >= 1),            COUNT(DISTINCT event_id)), 3) AS pct_cost_tco,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_cost_tco >= 3),            COUNT(DISTINCT event_id)), 3) AS pct_cost_tco_3plus,

    ROUND(SAFE_DIVIDE(COUNTIF(theme_procurement_process >= 1), COUNT(DISTINCT event_id)), 3) AS pct_procurement,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_procurement_process >= 3), COUNT(DISTINCT event_id)), 3) AS pct_procurement_3plus,

    ROUND(SAFE_DIVIDE(COUNTIF(theme_risk_concern >= 1),        COUNT(DISTINCT event_id)), 3) AS pct_risk,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_internal_stakeholders >= 1), COUNT(DISTINCT event_id)), 3) AS pct_stakeholders,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_security_compliance >= 1), COUNT(DISTINCT event_id)), 3) AS pct_security,

    -- ── EVALUATION QUESTIONS ──────────────────────────────────
    ROUND(SAFE_DIVIDE(COUNTIF(theme_timeline >= 1),            COUNT(DISTINCT event_id)), 3) AS pct_timeline,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_integration_tech >= 1),    COUNT(DISTINCT event_id)), 3) AS pct_integration,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_integration_tech >= 3),    COUNT(DISTINCT event_id)), 3) AS pct_integration_3plus,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_implementation_services >= 1), COUNT(DISTINCT event_id)), 3) AS pct_impl_services,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_customization >= 1),       COUNT(DISTINCT event_id)), 3) AS pct_customization,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_scalability >= 1),         COUNT(DISTINCT event_id)), 3) AS pct_scalability,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_analytics_reporting >= 1), COUNT(DISTINCT event_id)), 3) AS pct_analytics,

    -- ── COMPETITIVE & MIGRATION ───────────────────────────────
    ROUND(SAFE_DIVIDE(COUNTIF(theme_competitive >= 1),         COUNT(DISTINCT event_id)), 3) AS pct_competitive,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_competitive >= 3),         COUNT(DISTINCT event_id)), 3) AS pct_competitive_3plus,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_migration >= 1),           COUNT(DISTINCT event_id)), 3) AS pct_migration,

    -- ── PRODUCT ───────────────────────────────────────────────
    ROUND(SAFE_DIVIDE(COUNTIF(theme_checkout_payments >= 1),   COUNT(DISTINCT event_id)), 3) AS pct_checkout,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_pos_omnichannel >= 1),     COUNT(DISTINCT event_id)), 3) AS pct_pos_omnichannel,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_b2b >= 1),                 COUNT(DISTINCT event_id)), 3) AS pct_b2b,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_international >= 1),       COUNT(DISTINCT event_id)), 3) AS pct_international,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_ai >= 1),                  COUNT(DISTINCT event_id)), 3) AS pct_ai,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_ai >= 3),                  COUNT(DISTINCT event_id)), 3) AS pct_ai_3plus,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_growth >= 1),              COUNT(DISTINCT event_id)), 3) AS pct_growth,

    -- ── APAC-SPECIFIC ─────────────────────────────────────────
    ROUND(SAFE_DIVIDE(COUNTIF(theme_local_payments >= 1),      COUNT(DISTINCT event_id)), 3) AS pct_local_payments,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_localisation >= 1),        COUNT(DISTINCT event_id)), 3) AS pct_localisation,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_marketplace_crossborder >= 1), COUNT(DISTINCT event_id)), 3) AS pct_marketplace_xborder

FROM call_themes_staged
WHERE apac_subregion != 'Other / Unclassified'
  AND stage_group IN ('Closed Won', 'Closed Lost')
GROUP BY 1, 2, 3
ORDER BY
    CASE apac_subregion
        WHEN 'ANZ'                   THEN 1
        WHEN 'Japan'                 THEN 2
        WHEN 'Greater China Region'  THEN 3
        WHEN 'Rest of Asia'          THEN 4
    END,
    stage_group,
    segment_bucket;





-- ============================================================
-- VARIANT A: Pipeline stage analysis (Early-Stage vs Late-Stage)
-- ============================================================
/*
SELECT
    apac_subregion,
    stage_group,
    COUNT(DISTINCT event_id) AS total_calls,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_cost_tco >= 1),            COUNT(DISTINCT event_id)), 3) AS pct_cost_tco,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_procurement_process >= 1), COUNT(DISTINCT event_id)), 3) AS pct_procurement,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_competitive >= 1),         COUNT(DISTINCT event_id)), 3) AS pct_competitive,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_integration_tech >= 1),    COUNT(DISTINCT event_id)), 3) AS pct_integration,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_risk_concern >= 1),        COUNT(DISTINCT event_id)), 3) AS pct_risk
FROM call_themes_staged
WHERE apac_subregion != 'Other / Unclassified'
  AND stage_group IN ('Early-Stage', 'Late-Stage', 'Closed Won', 'Closed Lost')
GROUP BY 1, 2
ORDER BY 1, CASE stage_group
    WHEN 'Early-Stage'  THEN 1
    WHEN 'Late-Stage'   THEN 2
    WHEN 'Closed Won'   THEN 3
    WHEN 'Closed Lost'  THEN 4 END;
*/





-- ============================================================
-- VARIANT B: Plus Acquisition breakout
-- ============================================================
/*
SELECT
    apac_subregion,
    stage_group,
    is_plus_acquisition,
    COUNT(DISTINCT event_id) AS total_calls,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_cost_tco >= 1), COUNT(DISTINCT event_id)), 3) AS pct_cost_tco,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_integration_tech >= 1), COUNT(DISTINCT event_id)), 3) AS pct_integration,
    ROUND(SAFE_DIVIDE(COUNTIF(theme_customization >= 1), COUNT(DISTINCT event_id)), 3) AS pct_customization
FROM call_themes_staged
WHERE apac_subregion != 'Other / Unclassified'
  AND stage_group IN ('Closed Won', 'Closed Lost')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
*/





-- ============================================================
-- VARIANT C: Verbatim prospect quotes for a specific theme
-- ============================================================
/*
SELECT
    call_date,
    apac_subregion,
    current_stage_name,
    speaker_text
FROM prospect_sentences
WHERE REGEXP_CONTAINS(LOWER(speaker_text),
    r'(?:\bai\b|artificial intelligence|machine learning|automat|sidekick|copilot|agentic)')
ORDER BY call_date DESC
LIMIT 200;
*/
