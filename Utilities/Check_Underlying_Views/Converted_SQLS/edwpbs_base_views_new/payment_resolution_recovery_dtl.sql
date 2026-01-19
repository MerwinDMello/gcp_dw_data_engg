-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_dtl AS SELECT
    payment_resolution_recovery_dtl.payment_resolution_recovery_id,
    payment_resolution_recovery_dtl.reporting_date,
    payment_resolution_recovery_dtl.rpt_freq_type_code,
    payment_resolution_recovery_dtl.payor_dw_id,
    payment_resolution_recovery_dtl.patient_dw_id,
    payment_resolution_recovery_dtl.company_code,
    payment_resolution_recovery_dtl.coid,
    payment_resolution_recovery_dtl.unit_num,
    payment_resolution_recovery_dtl.created_by_user_id,
    payment_resolution_recovery_dtl.created_date_time,
    payment_resolution_recovery_dtl.file_id,
    payment_resolution_recovery_dtl.invalid_ind,
    payment_resolution_recovery_dtl.month_id,
    payment_resolution_recovery_dtl.iplan_id,
    payment_resolution_recovery_dtl.pat_acct_num,
    payment_resolution_recovery_dtl.financial_class_sid,
    payment_resolution_recovery_dtl.patient_type_sid,
    payment_resolution_recovery_dtl.payment_date,
    payment_resolution_recovery_dtl.payor_sid,
    payment_resolution_recovery_dtl.reason_code_sid,
    payment_resolution_recovery_dtl.updated_by_user_id,
    payment_resolution_recovery_dtl.updated_by_date_time,
    payment_resolution_recovery_dtl.source_system_code,
    payment_resolution_recovery_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payment_resolution_recovery_dtl
;
