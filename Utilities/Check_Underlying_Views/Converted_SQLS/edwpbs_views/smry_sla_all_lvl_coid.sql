-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_sla_all_lvl_coid.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_all_lvl_coid AS SELECT
    b.parent_code,
    b.fact_lvl_code,
    a.service_type_name,
    a.month_id,
    a.child_code,
    a.corporate_code,
    a.metric_code,
    a.result_ind,
    a.change_date,
    a.user_id,
    ROUND(a.metric_actual_value, 5, 'ROUND_HALF_EVEN') AS metric_actual_value,
    ROUND(a.benchmark_value, 5, 'ROUND_HALF_EVEN') AS benchmark_value,
    a.metric_type_code,
    a.status_flag,
    ROUND(a.incentive_target_value, 3, 'ROUND_HALF_EVEN') AS incentive_target_value,
    a.incentive_result
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_month_scorecard AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND upper(a.child_code) = upper(b.coid)
     AND b.fact_lvl_code IN(
      3, 4, 5, 7
    )
;
