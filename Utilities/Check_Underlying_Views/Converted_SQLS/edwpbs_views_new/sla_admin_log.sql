-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sla_admin_log.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sla_admin_log AS SELECT
    sla_admin_log.service_type_name,
    sla_admin_log.fact_lvl_code,
    sla_admin_log.month_id,
    sla_admin_log.corporate_code,
    sla_admin_log.metric_code,
    sla_admin_log.metric_type_code,
    sla_admin_log.parent_code,
    sla_admin_log.child_code,
    sla_admin_log.metric_actual_value,
    sla_admin_log.metric_actual_value_old,
    sla_admin_log.benchmark_value,
    sla_admin_log.benchmark_value_old,
    sla_admin_log.incentive_target_value,
    sla_admin_log.incentive_target_value_old,
    sla_admin_log.status_flag,
    sla_admin_log.status_flag_old,
    sla_admin_log.change_date,
    sla_admin_log.user_id,
    sla_admin_log.comment_txt,
    sla_admin_log.screen_modified_flag,
    sla_admin_log.dw_last_update_datetime
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.sla_admin_log
;
