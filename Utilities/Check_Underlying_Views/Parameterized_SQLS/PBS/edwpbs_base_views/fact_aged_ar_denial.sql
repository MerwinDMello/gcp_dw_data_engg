-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_aged_ar_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.fact_aged_ar_denial
   OPTIONS(description='Maintains Aged AR of the Accounts  grouping by Patient type, Payor, Denial Code etc.')
  AS SELECT
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
      ROUND(fact_aged_ar_denial.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
      fact_aged_ar_denial.source_system_code,
      fact_aged_ar_denial.dw_last_update_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.fact_aged_ar_denial
  ;
