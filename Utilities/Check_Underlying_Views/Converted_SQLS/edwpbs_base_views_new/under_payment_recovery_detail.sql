-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/under_payment_recovery_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.under_payment_recovery_detail AS SELECT
    under_payment_recovery_detail.month_id,
    under_payment_recovery_detail.patient_dw_id,
    under_payment_recovery_detail.iplan_id,
    under_payment_recovery_detail.under_payment_recovery_date,
    under_payment_recovery_detail.company_code,
    under_payment_recovery_detail.coid,
    under_payment_recovery_detail.unit_num,
    under_payment_recovery_detail.pat_acct_num,
    under_payment_recovery_detail.under_payment_recovery_amt,
    under_payment_recovery_detail.source_system_code,
    under_payment_recovery_detail.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.under_payment_recovery_detail
;
