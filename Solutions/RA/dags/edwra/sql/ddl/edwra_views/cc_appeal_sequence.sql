-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_appeal_sequence.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_appeal_sequence
   OPTIONS(description='Concuity appeal sequences related to an open appeal row.')
  AS SELECT
      a.company_code,
      a.coid,
      a.patient_dw_id,
      a.payor_dw_id,
      a.iplan_insurance_order_num,
      a.appeal_num,
      a.appeal_seq_num,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.appeal_seq_begin_bal_amt,
      a.appeal_seq_current_bal_amt,
      a.appeal_seq_end_bal_amt,
      a.appeal_seq_deadline_date,
      a.appeal_seq_close_date_time,
      a.appeal_seq_root_cause_id,
      a.appeal_seq_root_cause_dtl_text,
      a.appeal_disp_code_id,
      a.appeal_code_id,
      a.appeal_seq_owner_user_id,
      a.appeal_seq_create_user_id,
      a.appeal_seq_create_date_time,
      a.appeal_seq_update_user_id,
      a.appeal_seq_update_date_time,
      a.appeal_disp_id_update_user_id,
      a.appeal_disp_id_date_time,
      a.vendor_cd,
      a.appeal_seq_reopen_user_id,
      a.appeal_seq_reopen_date_time,
      a.appeal_level_num,
      a.appeal_sent_date,
      a.prior_appeal_response_date,
      a.dw_last_update_date_time,
      a.source_system_code,
      a.appeal_receipt_date
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_appeal_sequence AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
