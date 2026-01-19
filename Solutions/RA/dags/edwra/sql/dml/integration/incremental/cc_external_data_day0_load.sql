DECLARE DUP_COUNT INT64;

-- Translation time: 2024-05-07T21:55:14.046638Z
-- Translation job ID: 3ecd2e60-f47e-47d4-8588-b8fa61bc9a2d
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/5kB2mk/input/cc_external_data_day0_load.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************
		Comment & Change History
	Developer: Pritam Tawale
       Name: CC_External_Data_Day0_Load.sql
       Date: 04/12/2023
**********************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND(  'App=RA_Group2_ETL;Job=CTDRA611;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- ---Diagnostic noprodjoin on for session;
 -- ---Diagnostic nohashjoin on for session;
 -- ---Diagnostic noviewfold on for session;
 -- ---Diagnostic noparallel on for session;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vtl CLUSTER BY concuity_schema,
                               unit_num,
                               pat_acct_num,
                               iplan_id AS
SELECT edr_export.concuity_schema,
       edr_export.unit_num,
       edr_export.pat_acct_num,
       edr_export.iplan_id,
       edr_export.iplan_order_num,
       edr_export.esl_level_1_desc,
       edr_export.esl_level_2_desc,
       edr_export.esl_level_3_desc,
       edr_export.esl_level_4_desc,
       edr_export.esl_level_5_desc,
       edr_export.chois_product_line_code,
       edr_export.chois_product_line_desc,
       edr_export.drg_medical_surgical_ind,
       edr_export.apr_drg_code,
       edr_export.apr_drg_grouper_name,
       edr_export.apr_severity_of_illness_desc,
       edr_export.apr_risk_of_mortality_desc,
       edr_export.payer_type_code,
       edr_export.sub_payor_group_id,
       edr_export.cond_code_xf_xg_ind,
       edr_export.cond_code_nu_ind,
       edr_export.cond_code_ne_ind,
       edr_export.cond_code_ns_ind,
       edr_export.cond_code_np_ind,
       edr_export.cond_code_no_ind,
       edr_export.treatment_authorization_num,
       edr_export.denial_in_midas_status,
       edr_export.midas_date_of_denial,
       edr_export.ptp_performed,
       edr_export.all_days_approved_ind,
       edr_export.cm_xf_ind,
       edr_export.cm_xg_ind,
       edr_export.cm_last_xf_code_applied_date,
       edr_export.cm_last_xg_code_applied_date,
       edr_export.midas_acct_num,
       edr_export.last_appeal_date,
       edr_export.last_appeal_status,
       edr_export.last_appeal_employee_id,
       edr_export.last_appeal_employee_name,
       edr_export.status_cause_name,
       edr_export.last_conc_review_disp,
       edr_export.midas_principal_payer_auth_num,
       edr_export.midas_principal_pyr_auth_type,
       edr_export.cm_last_iq_revi_crit_met_desc,
       edr_export.cm_last_iq_review_version_desc,
       edr_export.cm_last_iq_review_subset_desc,
       edr_export.pdu_determination_reason_desc,
       edr_export.ins1_payor_balance_amt,
       edr_export.ins2_payor_balance_amt,
       edr_export.ins3_payor_balance_amt,
       edr_export.first_doc_request_mr_date,
       edr_export.last_doc_request_mr_date,
       edr_export.first_doc_sent_mr_date,
       edr_export.last_doc_sent_mr_date,
       edr_export.first_doc_request_ib_date,
       edr_export.last_doc_request_ib_date,
       edr_export.first_doc_sent_ib_date,
       edr_export.last_doc_sent_ib_date,
       edr_export.first_doc_request_date,
       edr_export.first_doc_sent_date,
       edr_export.last_doc_request_date,
       edr_export.last_doc_sent_date,
       edr_export.first_doc_received_date,
       edr_export.last_doc_received_date,
       edr_export.first_doc_approved_date,
       edr_export.last_doc_approved_date,
       edr_export.first_doc_denied_date,
       edr_export.last_doc_denied_date,
       edr_export.covid_positive_flag,
       edr_export.refund_amt,
       edr_export.refund_create_date,
       edr_export.refund_requested_by,
       edr_export.patient_refund_amt,
       edr_export.patient_refund_create_date,
       edr_export.patient_refund_requested_by,
       edr_export.credit_status,
       edr_export.discrepancy_source_desc,
       edr_export.reimbursement_impact_desc,
       edr_export.discrepancy_date_time,
       edr_export.request_date_time,
       edr_export.reprocess_reason_text,
       edr_export.status_desc,
       edr_export.split_bill_ind,
       edr_export.last_scrted_appl_date_time,
       edr_export.scripted_overpayment_desc,
       edr_export.last_letter_sent_date_time,
       edr_export.account_id,
       edr_export.account_payor_id,
       edr_export.org_id,
       edr_export.first_ptp_completed_date,
       edr_export.first_strength_of_case,
       edr_export.last_ptp_completed_date,
       edr_export.last_strength_of_case,
       edr_export.total_ptp_closure_status_completed,
       edr_export.total_midnights,
       edr_export.total_inhouse_midnights,
       edr_export.gov_sec_tert_iplan,
       edr_export.takeback_follow_up_ltr_ind,
       edr_export.patient_refund_last_update_date_time,
       edr_export.payer_refund_last_update_date_time,
       edr_export.tmr_ma_ereq_auto_nonbillable_review_ind,
       edr_export.presentation_date,
       edr_export.artiva_claim_num,
       edr_export.recoup_date,
       edr_export.recoup_flag,
       edr_export.ground_transportation_ind,      
       edr_export.dna_clinical_rationale_1,
       edr_export.dna_clinical_rationale_2,
       edr_export.dna_clinical_rationale_3,
       edr_export.dna_clinical_rationale_4,
       edr_export.dna_clinical_rationale_5,
       edr_export.dna_clinical_rationale_6,
       edr_export.dna_clinical_rationale_6_notes,
       edr_export.dna_ip_only_proc_waterpark_ind,
       edr_export.dna_ip_only_proc_user_ind,
       edr_export.psr_agree_disagree_flag,
       edr_export.first_psr_note_date_time,
       edr_export.last_psr_note_date_time
FROM {{ params.param_parallon_ra_project_name }}.edwra_views.edr_export;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- COLLECT STATISTICS is not supported in this dialect.
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_full_load;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_full_load AS mt USING
  (SELECT DISTINCT substr(vtl.concuity_schema, 1, 10) AS concuity_schema,
                   vtl.unit_num AS unit_num,
                   vtl.pat_acct_num,
                   vtl.iplan_id,
                   vtl.iplan_order_num,
                   vtl.esl_level_1_desc,
                   vtl.esl_level_2_desc,
                   vtl.esl_level_3_desc,
                   vtl.esl_level_4_desc,
                   vtl.esl_level_5_desc,
                   vtl.chois_product_line_code AS chois_product_line_code,
                   vtl.chois_product_line_desc,
                   vtl.drg_medical_surgical_ind AS drg_medical_surgical_ind,
                   vtl.apr_drg_code AS apr_drg_code,
                   vtl.apr_drg_grouper_name AS apr_drg_grouper_name,
                   vtl.apr_severity_of_illness_desc AS apr_severity_of_illness_desc,
                   vtl.apr_risk_of_mortality_desc AS apr_risk_of_mortality_desc,
                   vtl.payer_type_code AS payer_type_code,
                   vtl.sub_payor_group_id AS sub_payor_group_id,
                   substr(vtl.cond_code_xf_xg_ind, 1, 2) AS cond_code_xf_xg_ind,
                   substr(vtl.cond_code_nu_ind, 1, 2) AS cond_code_nu_ind,
                   substr(vtl.cond_code_ne_ind, 1, 2) AS cond_code_ne_ind,
                   substr(vtl.cond_code_ns_ind, 1, 2) AS cond_code_ns_ind,
                   substr(vtl.cond_code_np_ind, 1, 2) AS cond_code_np_ind,
                   substr(vtl.cond_code_no_ind, 1, 2) AS cond_code_no_ind,
                   vtl.treatment_authorization_num,
                   substr(vtl.denial_in_midas_status, 1, 58) AS denial_in_midas_status,
                   vtl.midas_date_of_denial,
                   substr(vtl.ptp_performed, 1, 2) AS all_days_approved_ind,
                   substr(vtl.all_days_approved_ind, 1, 34) AS ptp_performed,
                   substr(vtl.cm_xf_ind, 1, 2) AS cm_xf_ind,
                   substr(vtl.cm_xg_ind, 1, 2) AS cm_xg_ind,
                   vtl.cm_last_xf_code_applied_date,
                   vtl.cm_last_xg_code_applied_date,
                   vtl.midas_acct_num,
                   vtl.last_appeal_date,
                   vtl.last_appeal_status,
                   vtl.last_appeal_employee_id,
                   vtl.last_appeal_employee_name,
                   vtl.status_cause_name,
                   vtl.last_conc_review_disp,
                   vtl.midas_principal_payer_auth_num,
                   substr(vtl.midas_principal_pyr_auth_type, 1, 160) AS midas_principal_pyr_auth_type,
                   vtl.cm_last_iq_revi_crit_met_desc,
                   vtl.cm_last_iq_review_version_desc,
                   vtl.cm_last_iq_review_subset_desc,
                   substr(vtl.pdu_determination_reason_desc, 1, 55) AS pdu_determination_reason_desc,
                   vtl.ins1_payor_balance_amt,
                   vtl.ins2_payor_balance_amt,
                   vtl.ins3_payor_balance_amt,
                   vtl.first_doc_request_mr_date,
                   vtl.last_doc_request_mr_date,
                   vtl.first_doc_sent_mr_date,
                   vtl.last_doc_sent_mr_date,
                   vtl.first_doc_request_ib_date,
                   vtl.last_doc_request_ib_date,
                   vtl.first_doc_sent_ib_date,
                   vtl.last_doc_sent_ib_date,
                   vtl.first_doc_request_date,
                   vtl.first_doc_sent_date,
                   vtl.last_doc_request_date,
                   vtl.last_doc_sent_date,
                   vtl.first_doc_received_date,
                   vtl.last_doc_received_date,
                   vtl.first_doc_approved_date,
                   vtl.last_doc_approved_date,
                   vtl.first_doc_denied_date,
                   vtl.last_doc_denied_date,
                   substr(vtl.covid_positive_flag, 1, 2) AS covid_positive_flag,
                   vtl.refund_amt,
                   vtl.refund_create_date,
                   vtl.refund_requested_by,
                   vtl.patient_refund_amt,
                   vtl.patient_refund_create_date,
                   vtl.patient_refund_requested_by,
                   vtl.credit_status,
                   vtl.discrepancy_source_desc,
                   vtl.reimbursement_impact_desc,
                   vtl.discrepancy_date_time,
                   vtl.request_date_time,
                   vtl.reprocess_reason_text,
                   vtl.status_desc,
                   substr(vtl.split_bill_ind, 1, 2) AS split_bill_ind,
                   vtl.last_scrted_appl_date_time,
                   vtl.scripted_overpayment_desc,
                   vtl.last_letter_sent_date_time,
                   vtl.account_id,
                   vtl.account_payor_id,
                   vtl.org_id,
                   vtl.first_ptp_completed_date,
                   substr(vtl.first_strength_of_case, 1, 100) AS first_strength_of_case,
                   vtl.last_ptp_completed_date,
                   substr(vtl.last_strength_of_case, 1, 100) AS last_strength_of_case,
                   vtl.total_ptp_closure_status_completed AS total_ptp_closure_status_completed,
                   vtl.total_midnights,
                   CAST(vtl.total_inhouse_midnights AS NUMERIC) AS total_inhouse_midnights,
                   substr(vtl.gov_sec_tert_iplan, 1, 2) AS gov_sec_tert_iplan,
                   substr(vtl.takeback_follow_up_ltr_ind, 1, 2) AS takeback_follow_up_ltr_ind,
                   vtl.patient_refund_last_update_date_time,
                   vtl.payer_refund_last_update_date_time,
                   vtl.tmr_ma_ereq_auto_nonbillable_review_ind,
                   vtl.presentation_date,
                   vtl.artiva_claim_num,
                   vtl.recoup_date,
                   vtl.recoup_flag,
                   vtl.ground_transportation_ind,
                   vtl.dna_clinical_rationale_1,
                   vtl.dna_clinical_rationale_2,
                   vtl.dna_clinical_rationale_3,
                   vtl.dna_clinical_rationale_4,
                   vtl.dna_clinical_rationale_5,
                   vtl.dna_clinical_rationale_6,
                   vtl.dna_clinical_rationale_6_notes,
                   vtl.dna_ip_only_proc_waterpark_ind,
                   vtl.dna_ip_only_proc_user_ind,
                   vtl.psr_agree_disagree_flag,
                   vtl.first_psr_note_date_time,
                   vtl.last_psr_note_date_time
   FROM vtl) AS ms ON upper(coalesce(mt.concuity_schema, '0')) = upper(coalesce(ms.concuity_schema, '0'))
AND upper(coalesce(mt.concuity_schema, '1')) = upper(coalesce(ms.concuity_schema, '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.iplan_order_num, 0) = coalesce(ms.iplan_order_num, 0)
     AND coalesce(mt.iplan_order_num, 1) = coalesce(ms.iplan_order_num, 1))
AND (upper(coalesce(mt.esl_level_1_desc, '0')) = upper(coalesce(ms.esl_level_1_desc, '0'))
     AND upper(coalesce(mt.esl_level_1_desc, '1')) = upper(coalesce(ms.esl_level_1_desc, '1')))
AND (upper(coalesce(mt.esl_level_2_desc, '0')) = upper(coalesce(ms.esl_level_2_desc, '0'))
     AND upper(coalesce(mt.esl_level_2_desc, '1')) = upper(coalesce(ms.esl_level_2_desc, '1')))
AND (upper(coalesce(mt.esl_level_3_desc, '0')) = upper(coalesce(ms.esl_level_3_desc, '0'))
     AND upper(coalesce(mt.esl_level_3_desc, '1')) = upper(coalesce(ms.esl_level_3_desc, '1')))
AND (upper(coalesce(mt.esl_level_4_desc, '0')) = upper(coalesce(ms.esl_level_4_desc, '0'))
     AND upper(coalesce(mt.esl_level_4_desc, '1')) = upper(coalesce(ms.esl_level_4_desc, '1')))
AND (upper(coalesce(mt.esl_level_5_desc, '0')) = upper(coalesce(ms.esl_level_5_desc, '0'))
     AND upper(coalesce(mt.esl_level_5_desc, '1')) = upper(coalesce(ms.esl_level_5_desc, '1')))
AND (upper(coalesce(mt.chois_product_line_code, '0')) = upper(coalesce(ms.chois_product_line_code, '0'))
     AND upper(coalesce(mt.chois_product_line_code, '1')) = upper(coalesce(ms.chois_product_line_code, '1')))
AND (upper(coalesce(mt.chois_product_line_desc, '0')) = upper(coalesce(ms.chois_product_line_desc, '0'))
     AND upper(coalesce(mt.chois_product_line_desc, '1')) = upper(coalesce(ms.chois_product_line_desc, '1')))
AND (upper(coalesce(mt.drg_medical_surgical_ind, '0')) = upper(coalesce(ms.drg_medical_surgical_ind, '0'))
     AND upper(coalesce(mt.drg_medical_surgical_ind, '1')) = upper(coalesce(ms.drg_medical_surgical_ind, '1')))
AND (upper(coalesce(mt.apr_drg_code, '0')) = upper(coalesce(ms.apr_drg_code, '0'))
     AND upper(coalesce(mt.apr_drg_code, '1')) = upper(coalesce(ms.apr_drg_code, '1')))
AND (upper(coalesce(mt.apr_drg_grouper_name, '0')) = upper(coalesce(ms.apr_drg_grouper_name, '0'))
     AND upper(coalesce(mt.apr_drg_grouper_name, '1')) = upper(coalesce(ms.apr_drg_grouper_name, '1')))
AND (upper(coalesce(mt.apr_severity_of_illness_desc, '0')) = upper(coalesce(ms.apr_severity_of_illness_desc, '0'))
     AND upper(coalesce(mt.apr_severity_of_illness_desc, '1')) = upper(coalesce(ms.apr_severity_of_illness_desc, '1')))
AND (upper(coalesce(mt.apr_risk_of_mortality_desc, '0')) = upper(coalesce(ms.apr_risk_of_mortality_desc, '0'))
     AND upper(coalesce(mt.apr_risk_of_mortality_desc, '1')) = upper(coalesce(ms.apr_risk_of_mortality_desc, '1')))
AND (upper(coalesce(mt.payer_type_code, '0')) = upper(coalesce(ms.payer_type_code, '0'))
     AND upper(coalesce(mt.payer_type_code, '1')) = upper(coalesce(ms.payer_type_code, '1')))
AND (upper(coalesce(mt.sub_payor_group_id, '0')) = upper(coalesce(ms.sub_payor_group_id, '0'))
     AND upper(coalesce(mt.sub_payor_group_id, '1')) = upper(coalesce(ms.sub_payor_group_id, '1')))
AND (upper(coalesce(mt.cond_code_xf_xg_ind, '0')) = upper(coalesce(ms.cond_code_xf_xg_ind, '0'))
     AND upper(coalesce(mt.cond_code_xf_xg_ind, '1')) = upper(coalesce(ms.cond_code_xf_xg_ind, '1')))
AND (upper(coalesce(mt.cond_code_nu_ind, '0')) = upper(coalesce(ms.cond_code_nu_ind, '0'))
     AND upper(coalesce(mt.cond_code_nu_ind, '1')) = upper(coalesce(ms.cond_code_nu_ind, '1')))
AND (upper(coalesce(mt.cond_code_ne_ind, '0')) = upper(coalesce(ms.cond_code_ne_ind, '0'))
     AND upper(coalesce(mt.cond_code_ne_ind, '1')) = upper(coalesce(ms.cond_code_ne_ind, '1')))
AND (upper(coalesce(mt.cond_code_ns_ind, '0')) = upper(coalesce(ms.cond_code_ns_ind, '0'))
     AND upper(coalesce(mt.cond_code_ns_ind, '1')) = upper(coalesce(ms.cond_code_ns_ind, '1')))
AND (upper(coalesce(mt.cond_code_np_ind, '0')) = upper(coalesce(ms.cond_code_np_ind, '0'))
     AND upper(coalesce(mt.cond_code_np_ind, '1')) = upper(coalesce(ms.cond_code_np_ind, '1')))
AND (upper(coalesce(mt.cond_code_no_ind, '0')) = upper(coalesce(ms.cond_code_no_ind, '0'))
     AND upper(coalesce(mt.cond_code_no_ind, '1')) = upper(coalesce(ms.cond_code_no_ind, '1')))
AND (upper(coalesce(mt.treatment_authorization_num, '0')) = upper(coalesce(ms.treatment_authorization_num, '0'))
     AND upper(coalesce(mt.treatment_authorization_num, '1')) = upper(coalesce(ms.treatment_authorization_num, '1')))
AND (upper(coalesce(mt.denial_in_midas_status, '0')) = upper(coalesce(ms.denial_in_midas_status, '0'))
     AND upper(coalesce(mt.denial_in_midas_status, '1')) = upper(coalesce(ms.denial_in_midas_status, '1')))
AND (coalesce(mt.midas_date_of_denial, DATE '1970-01-01') = coalesce(ms.midas_date_of_denial, DATE '1970-01-01')
     AND coalesce(mt.midas_date_of_denial, DATE '1970-01-02') = coalesce(ms.midas_date_of_denial, DATE '1970-01-02'))
AND (upper(coalesce(mt.all_days_approved_ind, '0')) = upper(coalesce(ms.all_days_approved_ind, '0'))
     AND upper(coalesce(mt.all_days_approved_ind, '1')) = upper(coalesce(ms.all_days_approved_ind, '1')))
AND (upper(coalesce(mt.ptp_performed, '0')) = upper(coalesce(ms.ptp_performed, '0'))
     AND upper(coalesce(mt.ptp_performed, '1')) = upper(coalesce(ms.ptp_performed, '1')))
AND (upper(coalesce(mt.cm_xf_ind, '0')) = upper(coalesce(ms.cm_xf_ind, '0'))
     AND upper(coalesce(mt.cm_xf_ind, '1')) = upper(coalesce(ms.cm_xf_ind, '1')))
AND (upper(coalesce(mt.cm_xg_ind, '0')) = upper(coalesce(ms.cm_xg_ind, '0'))
     AND upper(coalesce(mt.cm_xg_ind, '1')) = upper(coalesce(ms.cm_xg_ind, '1')))
AND (coalesce(mt.cm_last_xf_code_applied_date, DATE '1970-01-01') = coalesce(ms.cm_last_xf_code_applied_date, DATE '1970-01-01')
     AND coalesce(mt.cm_last_xf_code_applied_date, DATE '1970-01-02') = coalesce(ms.cm_last_xf_code_applied_date, DATE '1970-01-02'))
AND (coalesce(mt.cm_last_xg_code_applied_date, DATE '1970-01-01') = coalesce(ms.cm_last_xg_code_applied_date, DATE '1970-01-01')
     AND coalesce(mt.cm_last_xg_code_applied_date, DATE '1970-01-02') = coalesce(ms.cm_last_xg_code_applied_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.midas_acct_num, '0')) = upper(coalesce(ms.midas_acct_num, '0'))
     AND upper(coalesce(mt.midas_acct_num, '1')) = upper(coalesce(ms.midas_acct_num, '1')))
AND (coalesce(mt.last_appeal_date, DATE '1970-01-01') = coalesce(ms.last_appeal_date, DATE '1970-01-01')
     AND coalesce(mt.last_appeal_date, DATE '1970-01-02') = coalesce(ms.last_appeal_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.last_appeal_status, '0')) = upper(coalesce(ms.last_appeal_status, '0'))
     AND upper(coalesce(mt.last_appeal_status, '1')) = upper(coalesce(ms.last_appeal_status, '1')))
AND (upper(coalesce(mt.last_appeal_employee_id, '0')) = upper(coalesce(ms.last_appeal_employee_id, '0'))
     AND upper(coalesce(mt.last_appeal_employee_id, '1')) = upper(coalesce(ms.last_appeal_employee_id, '1')))
AND (upper(coalesce(mt.last_appeal_employee_name, '0')) = upper(coalesce(ms.last_appeal_employee_name, '0'))
     AND upper(coalesce(mt.last_appeal_employee_name, '1')) = upper(coalesce(ms.last_appeal_employee_name, '1')))
AND (upper(coalesce(mt.status_cause_name, '0')) = upper(coalesce(ms.status_cause_name, '0'))
     AND upper(coalesce(mt.status_cause_name, '1')) = upper(coalesce(ms.status_cause_name, '1')))
AND (upper(coalesce(mt.last_conc_review_disp, '0')) = upper(coalesce(ms.last_conc_review_disp, '0'))
     AND upper(coalesce(mt.last_conc_review_disp, '1')) = upper(coalesce(ms.last_conc_review_disp, '1')))
AND (upper(coalesce(mt.midas_principal_payer_auth_num, '0')) = upper(coalesce(ms.midas_principal_payer_auth_num, '0'))
     AND upper(coalesce(mt.midas_principal_payer_auth_num, '1')) = upper(coalesce(ms.midas_principal_payer_auth_num, '1')))
AND (upper(coalesce(mt.midas_principal_pyr_auth_type, '0')) = upper(coalesce(ms.midas_principal_pyr_auth_type, '0'))
     AND upper(coalesce(mt.midas_principal_pyr_auth_type, '1')) = upper(coalesce(ms.midas_principal_pyr_auth_type, '1')))
AND (upper(coalesce(mt.cm_last_iq_revi_crit_met_desc, '0')) = upper(coalesce(ms.cm_last_iq_revi_crit_met_desc, '0'))
     AND upper(coalesce(mt.cm_last_iq_revi_crit_met_desc, '1')) = upper(coalesce(ms.cm_last_iq_revi_crit_met_desc, '1')))
AND (upper(coalesce(mt.cm_last_iq_review_version_desc, '0')) = upper(coalesce(ms.cm_last_iq_review_version_desc, '0'))
     AND upper(coalesce(mt.cm_last_iq_review_version_desc, '1')) = upper(coalesce(ms.cm_last_iq_review_version_desc, '1')))
AND (upper(coalesce(mt.cm_last_iq_review_subset_desc, '0')) = upper(coalesce(ms.cm_last_iq_review_subset_desc, '0'))
     AND upper(coalesce(mt.cm_last_iq_review_subset_desc, '1')) = upper(coalesce(ms.cm_last_iq_review_subset_desc, '1')))
AND (upper(coalesce(mt.pdu_determination_reason_desc, '0')) = upper(coalesce(ms.pdu_determination_reason_desc, '0'))
     AND upper(coalesce(mt.pdu_determination_reason_desc, '1')) = upper(coalesce(ms.pdu_determination_reason_desc, '1')))
AND (coalesce(mt.ins1_payor_balance_amt, NUMERIC '0') = coalesce(ms.ins1_payor_balance_amt, NUMERIC '0')
     AND coalesce(mt.ins1_payor_balance_amt, NUMERIC '1') = coalesce(ms.ins1_payor_balance_amt, NUMERIC '1'))
AND (coalesce(mt.ins2_payor_balance_amt, NUMERIC '0') = coalesce(ms.ins2_payor_balance_amt, NUMERIC '0')
     AND coalesce(mt.ins2_payor_balance_amt, NUMERIC '1') = coalesce(ms.ins2_payor_balance_amt, NUMERIC '1'))
AND (coalesce(mt.ins3_payor_balance_amt, NUMERIC '0') = coalesce(ms.ins3_payor_balance_amt, NUMERIC '0')
     AND coalesce(mt.ins3_payor_balance_amt, NUMERIC '1') = coalesce(ms.ins3_payor_balance_amt, NUMERIC '1'))
AND (coalesce(mt.first_doc_request_mr_date, DATE '1970-01-01') = coalesce(ms.first_doc_request_mr_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_request_mr_date, DATE '1970-01-02') = coalesce(ms.first_doc_request_mr_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_request_mr_date, DATE '1970-01-01') = coalesce(ms.last_doc_request_mr_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_request_mr_date, DATE '1970-01-02') = coalesce(ms.last_doc_request_mr_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_sent_mr_date, DATE '1970-01-01') = coalesce(ms.first_doc_sent_mr_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_sent_mr_date, DATE '1970-01-02') = coalesce(ms.first_doc_sent_mr_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_sent_mr_date, DATE '1970-01-01') = coalesce(ms.last_doc_sent_mr_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_sent_mr_date, DATE '1970-01-02') = coalesce(ms.last_doc_sent_mr_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_request_ib_date, DATE '1970-01-01') = coalesce(ms.first_doc_request_ib_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_request_ib_date, DATE '1970-01-02') = coalesce(ms.first_doc_request_ib_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_request_ib_date, DATE '1970-01-01') = coalesce(ms.last_doc_request_ib_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_request_ib_date, DATE '1970-01-02') = coalesce(ms.last_doc_request_ib_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_sent_ib_date, DATE '1970-01-01') = coalesce(ms.first_doc_sent_ib_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_sent_ib_date, DATE '1970-01-02') = coalesce(ms.first_doc_sent_ib_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_sent_ib_date, DATE '1970-01-01') = coalesce(ms.last_doc_sent_ib_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_sent_ib_date, DATE '1970-01-02') = coalesce(ms.last_doc_sent_ib_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_request_date, DATE '1970-01-01') = coalesce(ms.first_doc_request_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_request_date, DATE '1970-01-02') = coalesce(ms.first_doc_request_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_request_date, DATE '1970-01-01') = coalesce(ms.first_doc_sent_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_request_date, DATE '1970-01-02') = coalesce(ms.first_doc_sent_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_sent_date, DATE '1970-01-01') = coalesce(ms.last_doc_request_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_sent_date, DATE '1970-01-02') = coalesce(ms.last_doc_request_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_sent_date, DATE '1970-01-01') = coalesce(ms.last_doc_sent_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_sent_date, DATE '1970-01-02') = coalesce(ms.last_doc_sent_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_received_date, DATE '1970-01-01') = coalesce(ms.first_doc_received_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_received_date, DATE '1970-01-02') = coalesce(ms.first_doc_received_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_received_date, DATE '1970-01-01') = coalesce(ms.last_doc_received_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_received_date, DATE '1970-01-02') = coalesce(ms.last_doc_received_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_approved_date, DATE '1970-01-01') = coalesce(ms.first_doc_approved_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_approved_date, DATE '1970-01-02') = coalesce(ms.first_doc_approved_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_approved_date, DATE '1970-01-01') = coalesce(ms.last_doc_approved_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_approved_date, DATE '1970-01-02') = coalesce(ms.last_doc_approved_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_denied_date, DATE '1970-01-01') = coalesce(ms.first_doc_denied_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_denied_date, DATE '1970-01-02') = coalesce(ms.first_doc_denied_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_denied_date, DATE '1970-01-01') = coalesce(ms.last_doc_denied_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_denied_date, DATE '1970-01-02') = coalesce(ms.last_doc_denied_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.covid_positive_flag, '0')) = upper(coalesce(ms.covid_positive_flag, '0'))
     AND upper(coalesce(mt.covid_positive_flag, '1')) = upper(coalesce(ms.covid_positive_flag, '1')))
AND (coalesce(mt.refund_amt, NUMERIC '0') = coalesce(ms.refund_amt, NUMERIC '0')
     AND coalesce(mt.refund_amt, NUMERIC '1') = coalesce(ms.refund_amt, NUMERIC '1'))
AND (coalesce(mt.refund_create_date, DATE '1970-01-01') = coalesce(ms.refund_create_date, DATE '1970-01-01')
     AND coalesce(mt.refund_create_date, DATE '1970-01-02') = coalesce(ms.refund_create_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.refund_requested_by, '0')) = upper(coalesce(ms.refund_requested_by, '0'))
     AND upper(coalesce(mt.refund_requested_by, '1')) = upper(coalesce(ms.refund_requested_by, '1')))
AND (coalesce(mt.patient_refund_amt, NUMERIC '0') = coalesce(ms.patient_refund_amt, NUMERIC '0')
     AND coalesce(mt.patient_refund_amt, NUMERIC '1') = coalesce(ms.patient_refund_amt, NUMERIC '1'))
AND (coalesce(mt.patient_refund_create_date, DATE '1970-01-01') = coalesce(ms.patient_refund_create_date, DATE '1970-01-01')
     AND coalesce(mt.patient_refund_create_date, DATE '1970-01-02') = coalesce(ms.patient_refund_create_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_refund_requested_by, '0')) = upper(coalesce(ms.patient_refund_requested_by, '0'))
     AND upper(coalesce(mt.patient_refund_requested_by, '1')) = upper(coalesce(ms.patient_refund_requested_by, '1')))
AND (upper(coalesce(mt.credit_status, '0')) = upper(coalesce(ms.credit_status, '0'))
     AND upper(coalesce(mt.credit_status, '1')) = upper(coalesce(ms.credit_status, '1')))
AND (upper(coalesce(mt.discrepancy_source_desc, '0')) = upper(coalesce(ms.discrepancy_source_desc, '0'))
     AND upper(coalesce(mt.discrepancy_source_desc, '1')) = upper(coalesce(ms.discrepancy_source_desc, '1')))
AND (upper(coalesce(mt.reimbursement_impact_desc, '0')) = upper(coalesce(ms.reimbursement_impact_desc, '0'))
     AND upper(coalesce(mt.reimbursement_impact_desc, '1')) = upper(coalesce(ms.reimbursement_impact_desc, '1')))
AND (coalesce(mt.discrepancy_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.discrepancy_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.discrepancy_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.discrepancy_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.request_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.request_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.request_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.request_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.reprocess_reason_text, '0')) = upper(coalesce(ms.reprocess_reason_text, '0'))
     AND upper(coalesce(mt.reprocess_reason_text, '1')) = upper(coalesce(ms.reprocess_reason_text, '1')))
AND (upper(coalesce(mt.status_desc, '0')) = upper(coalesce(ms.status_desc, '0'))
     AND upper(coalesce(mt.status_desc, '1')) = upper(coalesce(ms.status_desc, '1')))
AND (upper(coalesce(mt.split_bill_ind, '0')) = upper(coalesce(ms.split_bill_ind, '0'))
     AND upper(coalesce(mt.split_bill_ind, '1')) = upper(coalesce(ms.split_bill_ind, '1')))
AND (coalesce(mt.last_scrted_appl_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.last_scrted_appl_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.last_scrted_appl_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.last_scrted_appl_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.scripted_overpayment_desc, '0')) = upper(coalesce(ms.scripted_overpayment_desc, '0'))
     AND upper(coalesce(mt.scripted_overpayment_desc, '1')) = upper(coalesce(ms.scripted_overpayment_desc, '1')))
AND (coalesce(mt.last_letter_sent_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.last_letter_sent_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.last_letter_sent_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.last_letter_sent_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.account_id, NUMERIC '0') = coalesce(ms.account_id, NUMERIC '0')
     AND coalesce(mt.account_id, NUMERIC '1') = coalesce(ms.account_id, NUMERIC '1'))
AND (coalesce(mt.account_payor_id, NUMERIC '0') = coalesce(ms.account_payor_id, NUMERIC '0')
     AND coalesce(mt.account_payor_id, NUMERIC '1') = coalesce(ms.account_payor_id, NUMERIC '1'))
AND (coalesce(mt.org_id, NUMERIC '0') = coalesce(ms.org_id, NUMERIC '0')
     AND coalesce(mt.org_id, NUMERIC '1') = coalesce(ms.org_id, NUMERIC '1'))
AND (coalesce(mt.first_ptp_completed_date, DATETIME '1970-01-01 00:00:00') = coalesce(ms.first_ptp_completed_date, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.first_ptp_completed_date, DATETIME '1970-01-01 00:00:01') = coalesce(ms.first_ptp_completed_date, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.first_strength_of_case, '0')) = upper(coalesce(ms.first_strength_of_case, '0'))
     AND upper(coalesce(mt.first_strength_of_case, '1')) = upper(coalesce(ms.first_strength_of_case, '1')))
AND (coalesce(mt.last_ptp_completed_date, DATETIME '1970-01-01 00:00:00') = coalesce(ms.last_ptp_completed_date, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.last_ptp_completed_date, DATETIME '1970-01-01 00:00:01') = coalesce(ms.last_ptp_completed_date, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.last_strength_of_case, '0')) = upper(coalesce(ms.last_strength_of_case, '0'))
     AND upper(coalesce(mt.last_strength_of_case, '1')) = upper(coalesce(ms.last_strength_of_case, '1')))
AND (coalesce(mt.total_ptp_closure_status_completed, 0) = coalesce(ms.total_ptp_closure_status_completed, 0)
     AND coalesce(mt.total_ptp_closure_status_completed, 1) = coalesce(ms.total_ptp_closure_status_completed, 1))
AND (coalesce(mt.total_midnights, 0) = coalesce(ms.total_midnights, 0)
     AND coalesce(mt.total_midnights, 1) = coalesce(ms.total_midnights, 1))
AND (coalesce(mt.total_inhouse_midnights, NUMERIC '0') = coalesce(ms.total_inhouse_midnights, NUMERIC '0')
     AND coalesce(mt.total_inhouse_midnights, NUMERIC '1') = coalesce(ms.total_inhouse_midnights, NUMERIC '1'))
AND (upper(coalesce(mt.gov_sec_tert_iplan, '0')) = upper(coalesce(ms.gov_sec_tert_iplan, '0'))
     AND upper(coalesce(mt.gov_sec_tert_iplan, '1')) = upper(coalesce(ms.gov_sec_tert_iplan, '1')))
AND (upper(coalesce(mt.takeback_follow_up_ltr_ind, '0')) = upper(coalesce(ms.takeback_follow_up_ltr_ind, '0'))
     AND upper(coalesce(mt.takeback_follow_up_ltr_ind, '1')) = upper(coalesce(ms.takeback_follow_up_ltr_ind, '1')))
AND (coalesce(mt.patient_refund_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.patient_refund_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.patient_refund_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.patient_refund_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.payer_refund_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.payer_refund_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.payer_refund_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.payer_refund_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (concuity_schema,
        unit_num,
        pat_acct_num,
        iplan_id,
        iplan_order_num,
        esl_level_1_desc,
        esl_level_2_desc,
        esl_level_3_desc,
        esl_level_4_desc,
        esl_level_5_desc,
        chois_product_line_code,
        chois_product_line_desc,
        drg_medical_surgical_ind,
        apr_drg_code,
        apr_drg_grouper_name,
        apr_severity_of_illness_desc,
        apr_risk_of_mortality_desc,
        payer_type_code,
        sub_payor_group_id,
        cond_code_xf_xg_ind,
        cond_code_nu_ind,
        cond_code_ne_ind,
        cond_code_ns_ind,
        cond_code_np_ind,
        cond_code_no_ind,
        treatment_authorization_num,
        denial_in_midas_status,
        midas_date_of_denial,
        all_days_approved_ind,
        ptp_performed,
        cm_xf_ind,
        cm_xg_ind,
        cm_last_xf_code_applied_date,
        cm_last_xg_code_applied_date,
        midas_acct_num,
        last_appeal_date,
        last_appeal_status,
        last_appeal_employee_id,
        last_appeal_employee_name,
        status_cause_name,
        last_conc_review_disp,
        midas_principal_payer_auth_num,
        midas_principal_pyr_auth_type,
        cm_last_iq_revi_crit_met_desc,
        cm_last_iq_review_version_desc,
        cm_last_iq_review_subset_desc,
        pdu_determination_reason_desc,
        ins1_payor_balance_amt,
        ins2_payor_balance_amt,
        ins3_payor_balance_amt,
        first_doc_request_mr_date,
        last_doc_request_mr_date,
        first_doc_sent_mr_date,
        last_doc_sent_mr_date,
        first_doc_request_ib_date,
        last_doc_request_ib_date,
        first_doc_sent_ib_date,
        last_doc_sent_ib_date,
        first_doc_request_date,
        last_doc_request_date,
        first_doc_sent_date,
        last_doc_sent_date,
        first_doc_received_date,
        last_doc_received_date,
        first_doc_approved_date,
        last_doc_approved_date,
        first_doc_denied_date,
        last_doc_denied_date,
        covid_positive_flag,
        refund_amt,
        refund_create_date,
        refund_requested_by,
        patient_refund_amt,
        patient_refund_create_date,
        patient_refund_requested_by,
        credit_status,
        discrepancy_source_desc,
        reimbursement_impact_desc,
        discrepancy_date_time,
        request_date_time,
        reprocess_reason_text,
        status_desc,
        split_bill_ind,
        last_scrted_appl_date_time,
        scripted_overpayment_desc,
        last_letter_sent_date_time,
        account_id,
        account_payor_id,
        org_id,
        first_ptp_completed_date,
        first_strength_of_case,
        last_ptp_completed_date,
        last_strength_of_case,
        total_ptp_closure_status_completed,
        total_midnights,
        total_inhouse_midnights,
        gov_sec_tert_iplan,
        takeback_follow_up_ltr_ind,
        patient_refund_last_update_date_time,
        payer_refund_last_update_date_time,
        tmr_ma_ereq_auto_nonbillable_review_ind,
        presentation_date,
        artiva_claim_num,
        recoup_date,
        recoup_flag,
        ground_transportation_ind,
        dna_clinical_rationale_1,
        dna_clinical_rationale_2,
        dna_clinical_rationale_3,
        dna_clinical_rationale_4,
        dna_clinical_rationale_5,
        dna_clinical_rationale_6,
        dna_clinical_rationale_6_notes,
        dna_ip_only_proc_waterpark_ind,
        dna_ip_only_proc_user_ind,
        psr_agree_disagree_flag,
        first_psr_note_date_time,
        last_psr_note_date_time)
VALUES (ms.concuity_schema, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.iplan_order_num, ms.esl_level_1_desc, ms.esl_level_2_desc, ms.esl_level_3_desc, ms.esl_level_4_desc, ms.esl_level_5_desc, ms.chois_product_line_code, ms.chois_product_line_desc, ms.drg_medical_surgical_ind, ms.apr_drg_code, ms.apr_drg_grouper_name, ms.apr_severity_of_illness_desc, ms.apr_risk_of_mortality_desc, ms.payer_type_code, ms.sub_payor_group_id, ms.cond_code_xf_xg_ind, ms.cond_code_nu_ind, ms.cond_code_ne_ind, ms.cond_code_ns_ind, ms.cond_code_np_ind, ms.cond_code_no_ind, ms.treatment_authorization_num, ms.denial_in_midas_status, ms.midas_date_of_denial, ms.all_days_approved_ind, ms.ptp_performed, ms.cm_xf_ind, ms.cm_xg_ind, ms.cm_last_xf_code_applied_date, ms.cm_last_xg_code_applied_date, ms.midas_acct_num, ms.last_appeal_date, ms.last_appeal_status, ms.last_appeal_employee_id, ms.last_appeal_employee_name, ms.status_cause_name, ms.last_conc_review_disp, ms.midas_principal_payer_auth_num, ms.midas_principal_pyr_auth_type, ms.cm_last_iq_revi_crit_met_desc, ms.cm_last_iq_review_version_desc, ms.cm_last_iq_review_subset_desc, ms.pdu_determination_reason_desc, ms.ins1_payor_balance_amt, ms.ins2_payor_balance_amt, ms.ins3_payor_balance_amt, ms.first_doc_request_mr_date, ms.last_doc_request_mr_date, ms.first_doc_sent_mr_date, ms.last_doc_sent_mr_date, ms.first_doc_request_ib_date, ms.last_doc_request_ib_date, ms.first_doc_sent_ib_date, ms.last_doc_sent_ib_date, ms.first_doc_request_date, ms.first_doc_sent_date, ms.last_doc_request_date, ms.last_doc_sent_date, ms.first_doc_received_date, ms.last_doc_received_date, ms.first_doc_approved_date, ms.last_doc_approved_date, ms.first_doc_denied_date, ms.last_doc_denied_date, ms.covid_positive_flag, ms.refund_amt, ms.refund_create_date, ms.refund_requested_by, ms.patient_refund_amt, ms.patient_refund_create_date, ms.patient_refund_requested_by, ms.credit_status, ms.discrepancy_source_desc, ms.reimbursement_impact_desc, ms.discrepancy_date_time, ms.request_date_time, ms.reprocess_reason_text, ms.status_desc, ms.split_bill_ind, ms.last_scrted_appl_date_time, ms.scripted_overpayment_desc, ms.last_letter_sent_date_time, ms.account_id, ms.account_payor_id, ms.org_id, ms.first_ptp_completed_date, ms.first_strength_of_case, ms.last_ptp_completed_date, ms.last_strength_of_case, ms.total_ptp_closure_status_completed, ms.total_midnights, ms.total_inhouse_midnights, ms.gov_sec_tert_iplan, ms.takeback_follow_up_ltr_ind, ms.patient_refund_last_update_date_time, ms.payer_refund_last_update_date_time,  ms.tmr_ma_ereq_auto_nonbillable_review_ind, ms.presentation_date, ms.artiva_claim_num,ms.recoup_date,ms.recoup_flag,ms.ground_transportation_ind, ms.dna_clinical_rationale_1, ms.dna_clinical_rationale_2, ms.dna_clinical_rationale_3, ms.dna_clinical_rationale_4, ms.dna_clinical_rationale_5, ms.dna_clinical_rationale_6, ms.dna_clinical_rationale_6_notes, ms.dna_ip_only_proc_waterpark_ind, ms.dna_ip_only_proc_user_ind, ms.psr_agree_disagree_flag, ms.first_psr_note_date_time, ms.last_psr_note_date_time);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra_staging','CC_External_Data_Full_Load');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;