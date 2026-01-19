-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_appeal_sequence.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_appeal_sequence
   OPTIONS(description='Concuity appeal sequences related to an open appeal row.')
  AS SELECT
      cc_appeal_sequence.company_code,
      cc_appeal_sequence.coid,
      cc_appeal_sequence.patient_dw_id,
      cc_appeal_sequence.payor_dw_id,
      cc_appeal_sequence.iplan_insurance_order_num,
      cc_appeal_sequence.appeal_num,
      cc_appeal_sequence.appeal_seq_num,
      cc_appeal_sequence.unit_num,
      cc_appeal_sequence.pat_acct_num,
      cc_appeal_sequence.iplan_id,
      cc_appeal_sequence.appeal_seq_begin_bal_amt,
      cc_appeal_sequence.appeal_seq_current_bal_amt,
      cc_appeal_sequence.appeal_seq_end_bal_amt,
      cc_appeal_sequence.appeal_seq_deadline_date,
      cc_appeal_sequence.appeal_seq_close_date_time,
      cc_appeal_sequence.appeal_seq_root_cause_id,
      cc_appeal_sequence.appeal_seq_root_cause_dtl_text,
      cc_appeal_sequence.appeal_disp_code_id,
      cc_appeal_sequence.appeal_code_id,
      cc_appeal_sequence.appeal_seq_owner_user_id,
      cc_appeal_sequence.appeal_seq_create_user_id,
      cc_appeal_sequence.appeal_seq_create_date_time,
      cc_appeal_sequence.appeal_seq_update_user_id,
      cc_appeal_sequence.appeal_seq_update_date_time,
      cc_appeal_sequence.appeal_disp_id_update_user_id,
      cc_appeal_sequence.appeal_disp_id_date_time,
      cc_appeal_sequence.vendor_cd,
      cc_appeal_sequence.appeal_seq_reopen_user_id,
      cc_appeal_sequence.appeal_seq_reopen_date_time,
      cc_appeal_sequence.appeal_level_num,
      cc_appeal_sequence.appeal_sent_date,
      cc_appeal_sequence.prior_appeal_response_date,
      cc_appeal_sequence.dw_last_update_date_time,
      cc_appeal_sequence.source_system_code,
      cc_appeal_sequence.appeal_receipt_date
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
  ;
