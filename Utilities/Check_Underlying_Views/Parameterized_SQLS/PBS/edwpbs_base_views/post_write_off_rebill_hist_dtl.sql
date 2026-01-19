-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/post_write_off_rebill_hist_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.post_write_off_rebill_hist_dtl
   OPTIONS(description='This table contains all Post Write Off Rebill History Detail of the accounts')
  AS SELECT
      ROUND(post_write_off_rebill_hist_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      post_write_off_rebill_hist_dtl.denial_orig_age_month_id,
      post_write_off_rebill_hist_dtl.write_off_pe_date,
      post_write_off_rebill_hist_dtl.company_code,
      post_write_off_rebill_hist_dtl.coid,
      post_write_off_rebill_hist_dtl.unit_num,
      ROUND(post_write_off_rebill_hist_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      post_write_off_rebill_hist_dtl.midas_acct_num,
      ROUND(post_write_off_rebill_hist_dtl.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(post_write_off_rebill_hist_dtl.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      post_write_off_rebill_hist_dtl.iplan_id,
      post_write_off_rebill_hist_dtl.discharge_date,
      post_write_off_rebill_hist_dtl.ms_drg_med_surg_flag,
      post_write_off_rebill_hist_dtl.denial_escl_review_desc,
      post_write_off_rebill_hist_dtl.denial_escl_rebill_flag,
      post_write_off_rebill_hist_dtl.denial_escl_rebill_project_date,
      post_write_off_rebill_hist_dtl.procedure_code,
      ROUND(post_write_off_rebill_hist_dtl.initial_write_off_amt, 3, 'ROUND_HALF_EVEN') AS initial_write_off_amt,
      post_write_off_rebill_hist_dtl.source_system_code,
      post_write_off_rebill_hist_dtl.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.post_write_off_rebill_hist_dtl
  ;
