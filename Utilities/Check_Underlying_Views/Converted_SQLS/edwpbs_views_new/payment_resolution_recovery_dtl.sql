-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_resolution_recovery_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_resolution_recovery_dtl AS SELECT
    a.payment_resolution_recovery_id,
    a.reporting_date,
    a.rpt_freq_type_code,
    a.payor_dw_id,
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.created_by_user_id,
    a.created_date_time,
    a.file_id,
    a.invalid_ind,
    a.month_id,
    a.iplan_id,
    a.pat_acct_num,
    a.financial_class_sid,
    a.patient_type_sid,
    a.payment_date,
    a.payor_sid,
    a.reason_code_sid,
    a.updated_by_user_id,
    a.updated_by_date_time,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
