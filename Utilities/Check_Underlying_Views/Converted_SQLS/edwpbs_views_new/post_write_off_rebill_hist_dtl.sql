-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/post_write_off_rebill_hist_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.post_write_off_rebill_hist_dtl AS SELECT
    a.patient_dw_id,
    a.denial_orig_age_month_id,
    a.write_off_pe_date,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.midas_acct_num,
    a.payor_dw_id,
    a.financial_class_code,
    a.iplan_id,
    a.discharge_date,
    a.ms_drg_med_surg_flag,
    a.denial_escl_review_desc,
    a.denial_escl_rebill_flag,
    a.denial_escl_rebill_project_date,
    a.procedure_code,
    a.initial_write_off_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.post_write_off_rebill_hist_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
