-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_pc_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_pc_metric_other AS SELECT
    smry_pc_metric_other.service_type_name,
    smry_pc_metric_other.fact_lvl_code,
    smry_pc_metric_other.parent_code,
    smry_pc_metric_other.child_code,
    smry_pc_metric_other.ytd_month_ind,
    smry_pc_metric_other.month_id,
    ROUND(smry_pc_metric_other.overturned_denial_amt, 3, 'ROUND_HALF_EVEN') AS overturned_denial_amt,
    ROUND(smry_pc_metric_other.unpay_recovery_amt, 3, 'ROUND_HALF_EVEN') AS unpay_recovery_amt,
    ROUND(smry_pc_metric_other.over_under_amt, 3, 'ROUND_HALF_EVEN') AS over_under_amt,
    ROUND(smry_pc_metric_other.exp_rbmt_amt, 3, 'ROUND_HALF_EVEN') AS exp_rbmt_amt,
    smry_pc_metric_other.dw_last_update_date_time,
    smry_pc_metric_other.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_pc_metric_other
;
