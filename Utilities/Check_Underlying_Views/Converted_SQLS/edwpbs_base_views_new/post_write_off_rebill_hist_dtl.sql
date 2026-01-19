-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/post_write_off_rebill_hist_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.post_write_off_rebill_hist_dtl AS SELECT
    post_write_off_rebill_hist_dtl.patient_dw_id,
    post_write_off_rebill_hist_dtl.denial_orig_age_month_id,
    post_write_off_rebill_hist_dtl.write_off_pe_date,
    post_write_off_rebill_hist_dtl.company_code,
    post_write_off_rebill_hist_dtl.coid,
    post_write_off_rebill_hist_dtl.unit_num,
    post_write_off_rebill_hist_dtl.pat_acct_num,
    post_write_off_rebill_hist_dtl.midas_acct_num,
    post_write_off_rebill_hist_dtl.payor_dw_id,
    post_write_off_rebill_hist_dtl.financial_class_code,
    post_write_off_rebill_hist_dtl.iplan_id,
    post_write_off_rebill_hist_dtl.discharge_date,
    post_write_off_rebill_hist_dtl.ms_drg_med_surg_flag,
    post_write_off_rebill_hist_dtl.denial_escl_review_desc,
    post_write_off_rebill_hist_dtl.denial_escl_rebill_flag,
    post_write_off_rebill_hist_dtl.denial_escl_rebill_project_date,
    post_write_off_rebill_hist_dtl.procedure_code,
    post_write_off_rebill_hist_dtl.initial_write_off_amt,
    post_write_off_rebill_hist_dtl.source_system_code,
    post_write_off_rebill_hist_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.post_write_off_rebill_hist_dtl
;
