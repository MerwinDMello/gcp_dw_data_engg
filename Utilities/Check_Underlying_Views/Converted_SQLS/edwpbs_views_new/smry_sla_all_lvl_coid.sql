-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    a.metric_actual_value,
    a.benchmark_value,
    a.metric_type_code,
    a.status_flag,
    a.incentive_target_value,
    a.incentive_result
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.smry_sla_month_scorecard AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.child_code = b.coid
     AND b.fact_lvl_code IN(
      3, 4, 5, 7
    )
;
