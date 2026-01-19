-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_aged_ar_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_aged_ar_denial AS SELECT
    a.coid,
    a.company_code,
    a.pe_date,
    a.unit_num_sid,
    a.time_id,
    a.age_month_sid,
    a.patient_type_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.account_status_sid,
    a.payor_sequence_sid,
    a.denial_code_sid,
    a.rcm_msr_sid,
    a.transaction_amt,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_aged_ar_denial AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
