-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_pc_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_pc_metric_other AS SELECT
    smry_pc_metric_other.service_type_name,
    smry_pc_metric_other.fact_lvl_code,
    smry_pc_metric_other.parent_code,
    smry_pc_metric_other.child_code,
    smry_pc_metric_other.ytd_month_ind,
    smry_pc_metric_other.month_id,
    smry_pc_metric_other.overturned_denial_amt,
    smry_pc_metric_other.unpay_recovery_amt,
    smry_pc_metric_other.over_under_amt,
    smry_pc_metric_other.exp_rbmt_amt,
    smry_pc_metric_other.dw_last_update_date_time,
    smry_pc_metric_other.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_pc_metric_other
;
