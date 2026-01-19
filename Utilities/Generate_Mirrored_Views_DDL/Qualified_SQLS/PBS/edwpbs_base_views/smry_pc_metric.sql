-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_pc_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_pc_metric AS SELECT
    smry_pc_metric.service_type_name,
    smry_pc_metric.fact_lvl_code,
    smry_pc_metric.parent_code,
    smry_pc_metric.child_code,
    smry_pc_metric.ytd_month_ind,
    smry_pc_metric.month_id,
    smry_pc_metric.payor_short_name,
    smry_pc_metric.denial_short_desc,
    ROUND(smry_pc_metric.unpay_inv_amt, 3, 'ROUND_HALF_EVEN') AS unpay_inv_amt,
    ROUND(smry_pc_metric.unpay_reason_inv_amt, 3, 'ROUND_HALF_EVEN') AS unpay_reason_inv_amt,
    ROUND(smry_pc_metric.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
    ROUND(smry_pc_metric.credit_balance_amt, 3, 'ROUND_HALF_EVEN') AS credit_balance_amt,
    ROUND(smry_pc_metric.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
    ROUND(smry_pc_metric.wo_denial_amt, 3, 'ROUND_HALF_EVEN') AS wo_denial_amt,
    smry_pc_metric.dw_last_update_date_time,
    smry_pc_metric.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_pc_metric
;
