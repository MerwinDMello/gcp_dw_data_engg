-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/post_write_off_rebill_hist_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.post_write_off_rebill_hist_dtl
   OPTIONS(description='This table contains all Post Write Off Rebill History Detail of the accounts')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.denial_orig_age_month_id,
      a.write_off_pe_date,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.midas_acct_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.iplan_id,
      a.discharge_date,
      a.ms_drg_med_surg_flag,
      a.denial_escl_review_desc,
      a.denial_escl_rebill_flag,
      a.denial_escl_rebill_project_date,
      a.procedure_code,
      ROUND(a.initial_write_off_amt, 3, 'ROUND_HALF_EVEN') AS initial_write_off_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.post_write_off_rebill_hist_dtl AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
