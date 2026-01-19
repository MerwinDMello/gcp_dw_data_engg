-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.net_revenue_impact_denial
   OPTIONS(description='Daily Denial data to capture net revenue impact.')
  AS SELECT
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      a.iplan_insurance_order_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.denial_status_code,
      a.patient_type_code,
      ROUND(a.write_off_denial_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_amt,
      ROUND(a.overturned_denial_amt, 3, 'ROUND_HALF_EVEN') AS overturned_denial_amt,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.net_revenue_impact_denial AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
