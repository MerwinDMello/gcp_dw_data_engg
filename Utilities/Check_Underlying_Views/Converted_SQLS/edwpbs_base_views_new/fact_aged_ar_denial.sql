-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_aged_ar_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_aged_ar_denial AS SELECT
    fact_aged_ar_denial.coid,
    fact_aged_ar_denial.company_code,
    fact_aged_ar_denial.pe_date,
    fact_aged_ar_denial.unit_num_sid,
    fact_aged_ar_denial.time_id,
    fact_aged_ar_denial.age_month_sid,
    fact_aged_ar_denial.patient_type_sid,
    fact_aged_ar_denial.payor_financial_class_sid,
    fact_aged_ar_denial.payor_sid,
    fact_aged_ar_denial.account_status_sid,
    fact_aged_ar_denial.payor_sequence_sid,
    fact_aged_ar_denial.denial_code_sid,
    fact_aged_ar_denial.rcm_msr_sid,
    fact_aged_ar_denial.transaction_amt,
    fact_aged_ar_denial.source_system_code,
    fact_aged_ar_denial.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_aged_ar_denial
;
