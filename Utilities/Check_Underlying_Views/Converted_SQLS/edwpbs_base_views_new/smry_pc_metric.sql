-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_pc_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_pc_metric AS SELECT
    smry_pc_metric.service_type_name,
    smry_pc_metric.fact_lvl_code,
    smry_pc_metric.parent_code,
    smry_pc_metric.child_code,
    smry_pc_metric.ytd_month_ind,
    smry_pc_metric.month_id,
    smry_pc_metric.payor_short_name,
    smry_pc_metric.denial_short_desc,
    smry_pc_metric.unpay_inv_amt,
    smry_pc_metric.unpay_reason_inv_amt,
    smry_pc_metric.refund_amt,
    smry_pc_metric.credit_balance_amt,
    smry_pc_metric.ending_balance_amt,
    smry_pc_metric.wo_denial_amt,
    smry_pc_metric.dw_last_update_date_time,
    smry_pc_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_pc_metric
;
