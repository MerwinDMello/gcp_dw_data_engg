-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_patient_account_audit.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_patient_account_audit
   OPTIONS(description='Display Audits and Refund information tied to a Concuity accounts.')
  AS SELECT
      a.patient_dw_id,
      a.payor_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.pat_acct_audit_id,
      a.acct_payor_id,
      a.claim_num,
      a.record_id,
      a.record_type_txt,
      a.corp_control_txt,
      a.ssc_control_txt,
      a.corp_status_date,
      a.audit_eligibility_txt,
      a.audit_status_txt,
      a.pre_audit_type_txt,
      a.post_audit_type_txt,
      a.other_audit_type_desc,
      a.audit_status_date,
      a.prev_audit_drg_nm,
      a.post_audit_drg_nm,
      a.payer_of_audit_txt,
      a.audit_schedule_date,
      a.expt_audit_fee_amt,
      a.act_audit_fee_coll_amt,
      a.audit_location_txt,
      a.final_audit_outcome_txt,
      a.work_again_date,
      a.service_start_date,
      a.service_end_date,
      a.corp_status_desc,
      a.addl_project_desc,
      a.finding_letter_result_txt,
      a.initiation_date,
      a.letter_date,
      a.response_date,
      a.mr_trk_id,
      a.mr_req_txt,
      a.mr_rls_req_to_roi_date,
      a.mr_rls_method_txt,
      a.mr_rls_date,
      a.mr_miss_notf_date,
      a.miss_doc_req_txt,
      a.payer_drg_code,
      a.payer_ref_id,
      a.risk_amt,
      a.disc_amt,
      a.refund_req_amt,
      a.refund_amt,
      a.refund_req_reason_txt,
      a.refund_status_txt,
      a.refund_status_date,
      a.final_refund_outcome_txt,
      a.creation_date,
      a.creation_user_id,
      a.modification_date,
      a.modification_user_id,
      a.map_to_record_type_txt,
      a.map_to_record_id,
      a.refund_prcs_tool_status_txt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_patient_account_audit AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
