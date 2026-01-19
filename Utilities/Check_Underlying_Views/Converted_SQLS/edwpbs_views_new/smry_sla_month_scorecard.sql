-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_sla_month_scorecard.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_month_scorecard AS SELECT
    d.service_type_name,
    a.fact_lvl_code,
    a.month_id,
    a.corporate_code,
    a.metric_code,
    a.parent_code,
    a.child_code,
    variance_value AS variance_value,
    c.metric_formula_code,
    CASE
      WHEN variance_value >= 0 THEN 1
      WHEN variance_value IS NULL THEN CAST(NULL as INT64)
      ELSE 0
    END AS result_ind,
    b.incentive_target_value,
    CASE
      WHEN upper(c.metric_formula_code) = '>='
       AND b.incentive_target_value < a.metric_actual_value THEN '1'
      WHEN upper(c.metric_formula_code) = '<='
       AND a.metric_actual_value < b.incentive_target_value THEN '1'
      ELSE CAST(NULL as STRING)
    END AS incentive_result,
    a.change_date,
    a.user_id,
    a.metric_actual_value,
    b.benchmark_value,
    b.metric_type_code,
    b.status_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_sla_scorecard AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_metric AS b ON upper(trim(a.metric_code)) = upper(trim(b.metric_code))
     AND upper(a.corporate_code) = upper(b.corporate_code)
     AND a.parent_code = b.coid
     AND a.fact_lvl_code = 7
     AND upper(a.service_type_name) = 'HOSPITAL'
     AND a.month_id = b.month_id
     AND b.current_active_ind = 'Y'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_metric_desc AS c ON upper(trim(a.metric_code)) = upper(trim(c.metric_code))
     AND upper(a.corporate_code) = upper(c.corporate_code)
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_service_type AS d
    CROSS JOIN UNNEST(ARRAY[
      CASE
         upper(c.metric_formula_code)
        WHEN '>=' THEN a.metric_actual_value - b.benchmark_value
        WHEN '<=' THEN b.benchmark_value - a.metric_actual_value
      END
    ]) AS variance_value
;
