-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_aged_ar_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_aged_ar_denial
   OPTIONS(description='Maintains Aged AR of the Accounts  grouping by Patient type, Payor, Denial Code etc.')
  AS SELECT
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
      ROUND(a.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_aged_ar_denial AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
