DECLARE DUP_COUNT INT64;

-- Translation time: 2024-06-25T16:46:33.739935Z
-- Translation job ID: 7a31bed6-8084-47a8-acd0-2837fcc9831d
-- Source: gs://eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/2qjzvd/input/cc_external_data_full_load.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************
		Comment & Change History
	Developer: Pritam Tawale
       Name: CC_External_Data_Full_Load.sql
       Date: 03/02/2023
       Mod1: Waiting on following fields to be added to PA feed
	     ERISA Indicator (Patient Level)
	     ERISA Indicator (Liability Level)
       Mod2:Request to Murali to add the fields below to Claim Reprocessing table
		Case Number
		Patient Type
		From
		Through
		Bill Type
		Total Charges
		Prior Control Number
	Mod3: Remove data from history table and reload data from VTL into hist for new day processing.
	Mod4: Added new DNA fields
*********************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND(  'App=RA_Group2_ETL;Job=CTDRA609;');
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
       cast(edr_export.discrepancy_date_time as timestamp) AS discrepancy_date_time,
       cast(edr_export.request_date_time as timestamp) AS request_date_time,
       edr_export.reprocess_reason_text,
       edr_export.status_desc,
       edr_export.split_bill_ind,
       cast(edr_export.last_scrted_appl_date_time as timestamp) AS last_scrted_appl_date_time,
       edr_export.scripted_overpayment_desc,
       cast(edr_export.last_letter_sent_date_time as timestamp) AS last_letter_sent_date_time,
       edr_export.account_id,
       edr_export.account_payor_id,
       edr_export.org_id,
       cast(edr_export.first_ptp_completed_date as timestamp) AS first_ptp_completed_date,
       edr_export.first_strength_of_case,
       cast(edr_export.last_ptp_completed_date as timestamp) AS last_ptp_completed_date,
       edr_export.last_strength_of_case,
       edr_export.total_ptp_closure_status_completed,
       edr_export.total_midnights,
       edr_export.total_inhouse_midnights,
       edr_export.gov_sec_tert_iplan,
       edr_export.takeback_follow_up_ltr_ind,
       cast(edr_export.patient_refund_last_update_date_time as timestamp) AS patient_refund_last_update_date_time,
       cast(edr_export.payer_refund_last_update_date_time as timestamp) AS payer_refund_last_update_date_time,
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
       cast(edr_export.first_psr_note_date_time as timestamp) AS first_psr_note_date_time,
       cast(edr_export.last_psr_note_date_time as timestamp) AS last_psr_note_date_time
FROM {{ params.param_parallon_ra_views_dataset_name }}.edr_export;


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


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist
WHERE DATE(cc_external_data_cdc_hist.dw_last_update_date_time) = current_date('US/Central');


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist AS x USING
  (SELECT t1.*
   FROM vtl AS t1
   WHERE NOT EXISTS
       (SELECT NULL
        FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist AS t2
        WHERE upper(rtrim(t1.concuity_schema)) = upper(rtrim(t2.concuity_schema))
          AND upper(rtrim(t1.unit_num)) = upper(rtrim(t2.unit_num))
          AND t1.pat_acct_num = t2.pat_acct_num
          AND t1.iplan_id = t2.iplan_id
          AND t1.iplan_order_num = t2.iplan_order_num
          AND md5(IFNULL(t1.concuity_schema,'') || IFNULL(CAST(t1.unit_num AS STRING),'') || IFNULL(CAST(t1.pat_acct_num AS STRING),'') || IFNULL(CAST(t1.iplan_id AS STRING),'') || IFNULL(CAST(t1.iplan_order_num AS STRING),'') || IFNULL(t1.esl_level_1_desc,'') || IFNULL(t1.esl_level_2_desc,'') || IFNULL(t1.esl_level_3_desc,'') || IFNULL(t1.esl_level_4_desc,'') || IFNULL(t1.esl_level_5_desc,'') || IFNULL(CAST(t1.chois_product_line_code AS STRING),'') || IFNULL(t1.chois_product_line_desc,'') || IFNULL(CAST(t1.drg_medical_surgical_ind AS STRING),'') || IFNULL(CAST(t1.apr_drg_code AS STRING),'') || IFNULL(CAST(t1.apr_drg_grouper_name AS STRING),'') || IFNULL(CAST(t1.apr_severity_of_illness_desc AS STRING),'') || IFNULL(CAST(t1.apr_risk_of_mortality_desc AS STRING),'') || IFNULL(CAST(t1.payer_type_code AS STRING),'') || IFNULL(CAST(t1.sub_payor_group_id AS STRING),'') || IFNULL(t1.cond_code_xf_xg_ind,'') || IFNULL(t1.cond_code_nu_ind,'') || IFNULL(t1.cond_code_ne_ind,'') || IFNULL(t1.cond_code_ns_ind,'') || IFNULL(t1.cond_code_np_ind,'') || IFNULL(t1.cond_code_no_ind,'') || IFNULL(t1.treatment_authorization_num,'') || IFNULL(t1.denial_in_midas_status,'') || IFNULL(CAST(t1.midas_date_of_denial AS STRING),'') || IFNULL(t1.all_days_approved_ind,'') || IFNULL(t1.ptp_performed,'') || IFNULL(t1.cm_xf_ind,'') || IFNULL(t1.cm_xg_ind,'') || IFNULL(CAST(t1.cm_last_xf_code_applied_date AS STRING),'') || IFNULL(CAST(t1.cm_last_xg_code_applied_date AS STRING),'') || IFNULL(t1.midas_acct_num,'') || IFNULL(CAST(t1.last_appeal_date AS STRING),'') || IFNULL(t1.last_appeal_status,'') || IFNULL(t1.last_appeal_employee_id,'') || IFNULL(t1.last_appeal_employee_name,'') || IFNULL(t1.status_cause_name,'') || IFNULL(t1.last_conc_review_disp,'') || IFNULL(t1.midas_principal_payer_auth_num,'') || IFNULL(t1.midas_principal_pyr_auth_type,'') || IFNULL(t1.cm_last_iq_revi_crit_met_desc,'') || IFNULL(t1.cm_last_iq_review_version_desc,'') || IFNULL(t1.cm_last_iq_review_subset_desc,'') || IFNULL(t1.pdu_determination_reason_desc,'') || IFNULL(CAST(t1.ins1_payor_balance_amt AS STRING),'') || IFNULL(CAST(t1.ins2_payor_balance_amt AS STRING),'') || IFNULL(CAST(t1.ins3_payor_balance_amt AS STRING),'') || IFNULL(CAST(t1.first_doc_request_mr_date AS STRING),'') || IFNULL(CAST(t1.last_doc_request_mr_date AS STRING),'') || IFNULL(CAST(t1.first_doc_sent_mr_date AS STRING),'') || IFNULL(CAST(t1.last_doc_sent_mr_date AS STRING),'') || IFNULL(CAST(t1.first_doc_request_ib_date AS STRING),'') || IFNULL(CAST(t1.last_doc_request_ib_date AS STRING),'') || IFNULL(CAST(t1.first_doc_sent_ib_date AS STRING),'') || IFNULL(CAST(t1.last_doc_sent_ib_date AS STRING),'') || IFNULL(CAST(t1.first_doc_request_date AS STRING),'') || IFNULL(CAST(t1.last_doc_request_date AS STRING),'') || IFNULL(CAST(t1.first_doc_sent_date AS STRING),'') || IFNULL(CAST(t1.last_doc_sent_date AS STRING),'') || IFNULL(CAST(t1.first_doc_received_date AS STRING),'') || IFNULL(CAST(t1.last_doc_received_date AS STRING),'') || IFNULL(CAST(t1.first_doc_approved_date AS STRING),'') || IFNULL(CAST(t1.last_doc_approved_date AS STRING),'') || IFNULL(CAST(t1.first_doc_denied_date AS STRING),'') || IFNULL(CAST(t1.last_doc_denied_date AS STRING),'') || IFNULL(t1.covid_positive_flag,'') || IFNULL(CAST(t1.refund_amt AS STRING),'') || IFNULL(CAST(t1.refund_create_date AS STRING),'') || IFNULL(t1.refund_requested_by,'') || IFNULL(CAST(t1.patient_refund_amt AS STRING),'') || IFNULL(CAST(t1.patient_refund_create_date AS STRING),'') || IFNULL(t1.patient_refund_requested_by,'') || IFNULL(t1.credit_status,'') || IFNULL(t1.discrepancy_source_desc,'') || IFNULL(t1.reimbursement_impact_desc,'') || IFNULL(CAST(t1.discrepancy_date_time AS STRING),'') || IFNULL(CAST(t1.request_date_time AS STRING),'') || IFNULL(t1.reprocess_reason_text,'') || IFNULL(t1.status_desc,'') || IFNULL(t1.split_bill_ind,'') || IFNULL(CAST(t1.last_scrted_appl_date_time AS STRING),'') || IFNULL(t1.scripted_overpayment_desc,'') || IFNULL(CAST(t1.last_letter_sent_date_time AS STRING),'') || IFNULL(CAST(t1.account_id AS STRING),'') || IFNULL(CAST(t1.account_payor_id AS STRING),'') || IFNULL(CAST(t1.org_id AS STRING),'') || IFNULL(CAST(t1.first_ptp_completed_date AS STRING),'') || IFNULL(t1.first_strength_of_case,'') || IFNULL(CAST(t1.last_ptp_completed_date AS STRING),'') || IFNULL(t1.last_strength_of_case,'') || IFNULL(CAST(t1.total_ptp_closure_status_completed AS STRING),'') || IFNULL(CAST(t1.total_midnights AS STRING),'') || IFNULL(CAST(t1.total_inhouse_midnights AS STRING),'') || IFNULL(t1.gov_sec_tert_iplan,'') || IFNULL(t1.takeback_follow_up_ltr_ind,'') || IFNULL(CAST(t1.patient_refund_last_update_date_time AS STRING),'') || IFNULL(CAST(t1.payer_refund_last_update_date_time AS STRING),'') || IFNULL(t1.tmr_ma_ereq_auto_nonbillable_review_ind,'') || IFNULL(CAST(t1.presentation_date AS STRING),'') || IFNULL(t1.artiva_claim_num,'') || IFNULL(CAST(t1.recoup_date AS STRING),'') || IFNULL(t1.recoup_flag,'') || IFNULL(t1.ground_transportation_ind,'') || IFNULL(t1.dna_clinical_rationale_1,'') || IFNULL(t1.dna_clinical_rationale_2,'') || IFNULL(t1.dna_clinical_rationale_3,'') || IFNULL(t1.dna_clinical_rationale_4,'') || IFNULL(t1.dna_clinical_rationale_5,'') || IFNULL(t1.dna_clinical_rationale_6,'') || IFNULL(t1.dna_clinical_rationale_6_notes,'') || IFNULL(t1.dna_ip_only_proc_waterpark_ind,'') || IFNULL(t1.dna_ip_only_proc_user_ind,'') || IFNULL(t1.psr_agree_disagree_flag,'') || IFNULL(CAST(t1.first_psr_note_date_time AS STRING),'') ) = md5(IFNULL(t2.concuity_schema,'') || IFNULL(CAST(t2.unit_num AS STRING),'') || IFNULL(CAST(t2.pat_acct_num AS STRING),'') || IFNULL(CAST(t2.iplan_id AS STRING),'') || IFNULL(CAST(t2.iplan_order_num AS STRING),'') || IFNULL(t2.esl_level_1_desc,'') || IFNULL(t2.esl_level_2_desc,'') || IFNULL(t2.esl_level_3_desc,'') || IFNULL(t2.esl_level_4_desc,'') || IFNULL(t2.esl_level_5_desc,'') || IFNULL(CAST(t2.chois_product_line_code AS STRING),'') || IFNULL(t2.chois_product_line_desc,'') || IFNULL(CAST(t2.drg_medical_surgical_ind AS STRING),'') || IFNULL(CAST(t2.apr_drg_code AS STRING),'') || IFNULL(CAST(t2.apr_drg_grouper_name AS STRING),'') || IFNULL(CAST(t2.apr_severity_of_illness_desc AS STRING),'') || IFNULL(CAST(t2.apr_risk_of_mortality_desc AS STRING),'') || IFNULL(CAST(t2.payer_type_code AS STRING),'') || IFNULL(CAST(t2.sub_payor_group_id AS STRING),'') || IFNULL(t2.cond_code_xf_xg_ind,'') || IFNULL(t2.cond_code_nu_ind,'') || IFNULL(t2.cond_code_ne_ind,'') || IFNULL(t2.cond_code_ns_ind,'') || IFNULL(t2.cond_code_np_ind,'') || IFNULL(t2.cond_code_no_ind,'') || IFNULL(t2.treatment_authorization_num,'') || IFNULL(t2.denial_in_midas_status,'') || IFNULL(CAST(t2.midas_date_of_denial AS STRING),'') || IFNULL(t2.all_days_approved_ind,'') || IFNULL(t2.ptp_performed,'') || IFNULL(t2.cm_xf_ind,'') || IFNULL(t2.cm_xg_ind,'') || IFNULL(CAST(t2.cm_last_xf_code_applied_date AS STRING),'') || IFNULL(CAST(t2.cm_last_xg_code_applied_date AS STRING),'') || IFNULL(t2.midas_acct_num,'') || IFNULL(CAST(t2.last_appeal_date AS STRING),'') || IFNULL(t2.last_appeal_status,'') || IFNULL(t2.last_appeal_employee_id,'') || IFNULL(t2.last_appeal_employee_name,'') || IFNULL(t2.status_cause_name,'') || IFNULL(t2.last_conc_review_disp,'') || IFNULL(t2.midas_principal_payer_auth_num,'') || IFNULL(t2.midas_principal_pyr_auth_type,'') || IFNULL(t2.cm_last_iq_revi_crit_met_desc,'') || IFNULL(t2.cm_last_iq_review_version_desc,'') || IFNULL(t2.cm_last_iq_review_subset_desc,'') || IFNULL(t2.pdu_determination_reason_desc,'') || IFNULL(CAST(t2.ins1_payor_balance_amt AS STRING),'') || IFNULL(CAST(t2.ins2_payor_balance_amt AS STRING),'') || IFNULL(CAST(t2.ins3_payor_balance_amt AS STRING),'') || IFNULL(CAST(t2.first_doc_request_mr_date AS STRING),'') || IFNULL(CAST(t2.last_doc_request_mr_date AS STRING),'') || IFNULL(CAST(t2.first_doc_sent_mr_date AS STRING),'') || IFNULL(CAST(t2.last_doc_sent_mr_date AS STRING),'') || IFNULL(CAST(t2.first_doc_request_ib_date AS STRING),'') || IFNULL(CAST(t2.last_doc_request_ib_date AS STRING),'') || IFNULL(CAST(t2.first_doc_sent_ib_date AS STRING),'') || IFNULL(CAST(t2.last_doc_sent_ib_date AS STRING),'') || IFNULL(CAST(t2.first_doc_request_date AS STRING),'') || IFNULL(CAST(t2.last_doc_request_date AS STRING),'') || IFNULL(CAST(t2.first_doc_sent_date AS STRING),'') || IFNULL(CAST(t2.last_doc_sent_date AS STRING),'') || IFNULL(CAST(t2.first_doc_received_date AS STRING),'') || IFNULL(CAST(t2.last_doc_received_date AS STRING),'') || IFNULL(CAST(t2.first_doc_approved_date AS STRING),'') || IFNULL(CAST(t2.last_doc_approved_date AS STRING),'') || IFNULL(CAST(t2.first_doc_denied_date AS STRING),'') || IFNULL(CAST(t2.last_doc_denied_date AS STRING),'') || IFNULL(t2.covid_positive_flag,'') || IFNULL(CAST(t2.refund_amt AS STRING),'') || IFNULL(CAST(t2.refund_create_date AS STRING),'') || IFNULL(t2.refund_requested_by,'') || IFNULL(CAST(t2.patient_refund_amt AS STRING),'') || IFNULL(CAST(t2.patient_refund_create_date AS STRING),'') || IFNULL(t2.patient_refund_requested_by,'') || IFNULL(t2.credit_status,'') || IFNULL(t2.discrepancy_source_desc,'') || IFNULL(t2.reimbursement_impact_desc,'') || IFNULL(CAST(t2.discrepancy_date_time AS STRING),'') || IFNULL(CAST(t2.request_date_time AS STRING),'') || IFNULL(t2.reprocess_reason_text,'') || IFNULL(t2.status_desc,'') || IFNULL(t2.split_bill_ind,'') || IFNULL(CAST(t2.last_scrted_appl_date_time AS STRING),'') || IFNULL(t2.scripted_overpayment_desc,'') || IFNULL(CAST(t2.last_letter_sent_date_time AS STRING),'') || IFNULL(CAST(t2.account_id AS STRING),'') || IFNULL(CAST(t2.account_payor_id AS STRING),'') || IFNULL(CAST(t2.org_id AS STRING),'') || IFNULL(CAST(t2.first_ptp_completed_date AS STRING),'') || IFNULL(t2.first_strength_of_case,'') || IFNULL(CAST(t2.last_ptp_completed_date AS STRING),'') || IFNULL(t2.last_strength_of_case,'') || IFNULL(CAST(t2.total_ptp_closure_status_completed AS STRING),'') || IFNULL(CAST(t2.total_midnights AS STRING),'') || IFNULL(CAST(t2.total_inhouse_midnights AS STRING),'') || IFNULL(t2.gov_sec_tert_iplan,'') || IFNULL(t2.takeback_follow_up_ltr_ind,'') || IFNULL(CAST(t2.patient_refund_last_update_date_time AS STRING),'') || IFNULL(CAST(t2.payer_refund_last_update_date_time AS STRING),'') || IFNULL(t2.tmr_ma_ereq_auto_nonbillable_review_ind,'') || IFNULL(CAST(t2.presentation_date AS STRING),'') || IFNULL(t2.artiva_claim_num,'') || IFNULL(CAST(t2.recoup_date AS STRING),'') || IFNULL(t2.recoup_flag,'') || IFNULL(t2.ground_transportation_ind,'') || IFNULL(t2.dna_clinical_rationale_1,'') || IFNULL(t2.dna_clinical_rationale_2,'') || IFNULL(t2.dna_clinical_rationale_3,'') || IFNULL(t2.dna_clinical_rationale_4,'') || IFNULL(t2.dna_clinical_rationale_5,'') || IFNULL(t2.dna_clinical_rationale_6,'') || IFNULL(t2.dna_clinical_rationale_6_notes,'') || IFNULL(t2.dna_ip_only_proc_waterpark_ind,'') || IFNULL(t2.dna_ip_only_proc_user_ind,'') || IFNULL(t2.psr_agree_disagree_flag,'') || IFNULL(CAST(t2.first_psr_note_date_time AS STRING),'') ) ) 
          ) AS z ON upper(rtrim(x.concuity_schema)) = upper(rtrim(z.concuity_schema))
AND upper(rtrim(x.unit_num)) = upper(rtrim(z.unit_num))
AND x.pat_acct_num = z.pat_acct_num
AND x.iplan_id = z.iplan_id
AND x.iplan_order_num = z.iplan_order_num
AND x.account_payor_id = z.account_payor_id
AND x.account_id = z.account_id
AND x.org_id = z.org_id WHEN MATCHED THEN
UPDATE
SET esl_level_1_desc = z.esl_level_1_desc,
    esl_level_2_desc = z.esl_level_2_desc,
    esl_level_3_desc = z.esl_level_3_desc,
    esl_level_4_desc = z.esl_level_4_desc,
    esl_level_5_desc = z.esl_level_5_desc,
    chois_product_line_code = z.chois_product_line_code,
    chois_product_line_desc = z.chois_product_line_desc,
    drg_medical_surgical_ind = z.drg_medical_surgical_ind,
    apr_drg_code = z.apr_drg_code,
    apr_drg_grouper_name = z.apr_drg_grouper_name,
    apr_severity_of_illness_desc = z.apr_severity_of_illness_desc,
    apr_risk_of_mortality_desc = z.apr_risk_of_mortality_desc,
    payer_type_code = z.payer_type_code,
    sub_payor_group_id = z.sub_payor_group_id,
    cond_code_xf_xg_ind = substr(z.cond_code_xf_xg_ind, 1, 2),
    cond_code_nu_ind = substr(z.cond_code_nu_ind, 1, 2),
    cond_code_ne_ind = substr(z.cond_code_ne_ind, 1, 2),
    cond_code_ns_ind = substr(z.cond_code_ns_ind, 1, 2),
    cond_code_np_ind = substr(z.cond_code_np_ind, 1, 2),
    cond_code_no_ind = substr(z.cond_code_no_ind, 1, 2),
    treatment_authorization_num = z.treatment_authorization_num,
    denial_in_midas_status = substr(z.denial_in_midas_status, 1, 58),
    midas_date_of_denial = z.midas_date_of_denial,
    all_days_approved_ind = substr(z.all_days_approved_ind, 1, 2),
    ptp_performed = substr(z.ptp_performed, 1, 34),
    cm_xf_ind = substr(z.cm_xf_ind, 1, 2),
    cm_xg_ind = substr(z.cm_xg_ind, 1, 2),
    cm_last_xf_code_applied_date = z.cm_last_xf_code_applied_date,
    cm_last_xg_code_applied_date = z.cm_last_xg_code_applied_date,
    midas_acct_num = z.midas_acct_num,
    last_appeal_date = z.last_appeal_date,
    last_appeal_status = z.last_appeal_status,
    last_appeal_employee_id = z.last_appeal_employee_id,
    last_appeal_employee_name = z.last_appeal_employee_name,
    status_cause_name = z.status_cause_name,
    last_conc_review_disp = z.last_conc_review_disp,
    midas_principal_payer_auth_num = z.midas_principal_payer_auth_num,
    midas_principal_pyr_auth_type = substr(z.midas_principal_pyr_auth_type, 1, 160),
    cm_last_iq_revi_crit_met_desc = z.cm_last_iq_revi_crit_met_desc,
    cm_last_iq_review_version_desc = z.cm_last_iq_review_version_desc,
    cm_last_iq_review_subset_desc = z.cm_last_iq_review_subset_desc,
    pdu_determination_reason_desc = substr(z.pdu_determination_reason_desc, 1, 55),
    ins1_payor_balance_amt = z.ins1_payor_balance_amt,
    ins2_payor_balance_amt = z.ins2_payor_balance_amt,
    ins3_payor_balance_amt = z.ins3_payor_balance_amt,
    first_doc_request_mr_date = z.first_doc_request_mr_date,
    last_doc_request_mr_date = z.last_doc_request_mr_date,
    first_doc_sent_mr_date = z.first_doc_sent_mr_date,
    last_doc_sent_mr_date = z.last_doc_sent_mr_date,
    first_doc_request_ib_date = z.first_doc_request_ib_date,
    last_doc_request_ib_date = z.last_doc_request_ib_date,
    first_doc_sent_ib_date = z.first_doc_sent_ib_date,
    last_doc_sent_ib_date = z.last_doc_sent_ib_date,
    first_doc_request_date = z.first_doc_request_date,
    last_doc_request_date = z.last_doc_request_date,
    first_doc_sent_date = z.first_doc_sent_date,
    last_doc_sent_date = z.last_doc_sent_date,
    first_doc_received_date = z.first_doc_received_date,
    last_doc_received_date = z.last_doc_received_date,
    first_doc_approved_date = z.first_doc_approved_date,
    last_doc_approved_date = z.last_doc_approved_date,
    first_doc_denied_date = z.first_doc_denied_date,
    last_doc_denied_date = z.last_doc_denied_date,
    covid_positive_flag = substr(z.covid_positive_flag, 1, 2),
    refund_amt = z.refund_amt,
    refund_create_date = z.refund_create_date,
    refund_requested_by = z.refund_requested_by,
    patient_refund_amt = z.patient_refund_amt,
    patient_refund_create_date = z.patient_refund_create_date,
    patient_refund_requested_by = z.patient_refund_requested_by,
    credit_status = z.credit_status,
    discrepancy_source_desc = z.discrepancy_source_desc,
    reimbursement_impact_desc = z.reimbursement_impact_desc,
    discrepancy_date_time = cast(z.discrepancy_date_time as timestamp),
    request_date_time = cast(z.request_date_time as timestamp),
    reprocess_reason_text = z.reprocess_reason_text,
    status_desc = z.status_desc,
    split_bill_ind = substr(z.split_bill_ind, 1, 2),
    last_scrted_appl_date_time = cast(z.last_scrted_appl_date_time as timestamp),
    scripted_overpayment_desc = z.scripted_overpayment_desc,
    last_letter_sent_date_time = cast(z.last_letter_sent_date_time as timestamp),
    first_ptp_completed_date = cast(z.first_ptp_completed_date as timestamp),
    first_strength_of_case = substr(z.first_strength_of_case, 1, 100),
    last_ptp_completed_date = cast(z.last_ptp_completed_date as timestamp),
    last_strength_of_case = substr(z.last_strength_of_case, 1, 100),
    total_ptp_closure_status_completed = z.total_ptp_closure_status_completed,
    total_midnights = z.total_midnights,
    total_inhouse_midnights = CAST(z.total_inhouse_midnights AS INT64),
    gov_sec_tert_iplan = CAST(z.total_inhouse_midnights AS STRING),
    takeback_follow_up_ltr_ind = substr(z.takeback_follow_up_ltr_ind, 1, 2),
    patient_refund_last_update_date_time = cast(z.patient_refund_last_update_date_time as timestamp),
    payer_refund_last_update_date_time = cast(z.payer_refund_last_update_date_time as timestamp),
    tmr_ma_ereq_auto_nonbillable_review_ind = z.tmr_ma_ereq_auto_nonbillable_review_ind,
    presentation_date = z.presentation_date,
    artiva_claim_num = z.artiva_claim_num,
    recoup_date = z.recoup_date,
    recoup_flag = z.recoup_flag,
    ground_transportation_ind = z.ground_transportation_ind,
    dna_clinical_rationale_1 = z.dna_clinical_rationale_1,
    dna_clinical_rationale_2 = z.dna_clinical_rationale_2,
    dna_clinical_rationale_3 = z.dna_clinical_rationale_3,
    dna_clinical_rationale_4 = z.dna_clinical_rationale_4,
    dna_clinical_rationale_5 = z.dna_clinical_rationale_5,
    dna_clinical_rationale_6 = z.dna_clinical_rationale_6,
    dna_clinical_rationale_6_notes = z.dna_clinical_rationale_6_notes,
    dna_ip_only_proc_waterpark_ind = z.dna_ip_only_proc_waterpark_ind,
    dna_ip_only_proc_user_ind = z.dna_ip_only_proc_user_ind,
    psr_agree_disagree_flag = z.psr_agree_disagree_flag,
    first_psr_note_date_time = cast(z.first_psr_note_date_time as timestamp),
    last_psr_note_date_time = cast(z.last_psr_note_date_time as timestamp),
    dw_last_update_date_time = cast(datetime_trunc(current_datetime('US/Central'), SECOND) as timestamp) WHEN NOT MATCHED BY TARGET THEN
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
        last_psr_note_date_time,
        dw_last_update_date_time)
VALUES (substr(z.concuity_schema, 1, 10), z.unit_num, z.pat_acct_num, z.iplan_id, z.iplan_order_num, z.esl_level_1_desc, z.esl_level_2_desc, z.esl_level_3_desc, z.esl_level_4_desc, z.esl_level_5_desc, z.chois_product_line_code, z.chois_product_line_desc, z.drg_medical_surgical_ind, z.apr_drg_code, z.apr_drg_grouper_name, z.apr_severity_of_illness_desc, z.apr_risk_of_mortality_desc, z.payer_type_code, z.sub_payor_group_id, substr(z.cond_code_xf_xg_ind, 1, 2), substr(z.cond_code_nu_ind, 1, 2), substr(z.cond_code_ne_ind, 1, 2), substr(z.cond_code_ns_ind, 1, 2), substr(z.cond_code_np_ind, 1, 2), substr(z.cond_code_no_ind, 1, 2), z.treatment_authorization_num, substr(z.denial_in_midas_status, 1, 58), z.midas_date_of_denial, substr(z.all_days_approved_ind, 1, 2), substr(z.ptp_performed, 1, 34), substr(z.cm_xf_ind, 1, 2), substr(z.cm_xg_ind, 1, 2), z.cm_last_xf_code_applied_date, z.cm_last_xg_code_applied_date, z.midas_acct_num, z.last_appeal_date, z.last_appeal_status, z.last_appeal_employee_id, z.last_appeal_employee_name, z.status_cause_name, z.last_conc_review_disp, z.midas_principal_payer_auth_num, substr(z.midas_principal_pyr_auth_type, 1, 160), z.cm_last_iq_revi_crit_met_desc, z.cm_last_iq_review_version_desc, z.cm_last_iq_review_subset_desc, substr(z.pdu_determination_reason_desc, 1, 55), z.ins1_payor_balance_amt, z.ins2_payor_balance_amt, z.ins3_payor_balance_amt, z.first_doc_request_mr_date, z.last_doc_request_mr_date, z.first_doc_sent_mr_date, z.last_doc_sent_mr_date, z.first_doc_request_ib_date, z.last_doc_request_ib_date, z.first_doc_sent_ib_date, z.last_doc_sent_ib_date, z.first_doc_request_date, z.last_doc_request_date, z.first_doc_sent_date, z.last_doc_sent_date, z.first_doc_received_date, z.last_doc_received_date, z.first_doc_approved_date, z.last_doc_approved_date, z.first_doc_denied_date, z.last_doc_denied_date, substr(z.covid_positive_flag, 1, 2), z.refund_amt, z.refund_create_date, z.refund_requested_by, z.patient_refund_amt, z.patient_refund_create_date, z.patient_refund_requested_by, z.credit_status, z.discrepancy_source_desc, z.reimbursement_impact_desc, cast(z.discrepancy_date_time as timestamp), cast(z.request_date_time as timestamp), z.reprocess_reason_text, z.status_desc, substr(z.split_bill_ind, 1, 2), cast(z.last_scrted_appl_date_time as timestamp), z.scripted_overpayment_desc, cast(z.last_letter_sent_date_time as timestamp), z.account_id, z.account_payor_id, z.org_id, cast(z.first_ptp_completed_date as timestamp), substr(z.first_strength_of_case, 1, 100), cast(z.last_ptp_completed_date as timestamp), substr(z.last_strength_of_case, 1, 100), z.total_ptp_closure_status_completed, z.total_midnights, CAST(z.total_inhouse_midnights AS INT64), substr(z.gov_sec_tert_iplan, 1, 2), substr(z.takeback_follow_up_ltr_ind, 1, 2), cast(z.patient_refund_last_update_date_time as timestamp), cast(z.payer_refund_last_update_date_time as timestamp), z.tmr_ma_ereq_auto_nonbillable_review_ind, z.presentation_date, z.artiva_claim_num, z.recoup_date, z.recoup_flag, z.ground_transportation_ind, z.dna_clinical_rationale_1, z.dna_clinical_rationale_2, z.dna_clinical_rationale_3, z.dna_clinical_rationale_4, z.dna_clinical_rationale_5, z.dna_clinical_rationale_6, z.dna_clinical_rationale_6_notes, z.dna_ip_only_proc_waterpark_ind, z.dna_ip_only_proc_user_ind, z.psr_agree_disagree_flag, cast(z.first_psr_note_date_time as timestamp), cast(z.last_psr_note_date_time as timestamp), cast(datetime_trunc(current_datetime('US/Central'), SECOND) as timestamp));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra_staging','CC_External_Data_CDC_Hist');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc AS mt USING
  (SELECT DISTINCT cc_external_data_cdc_hist.concuity_schema,
                   cc_external_data_cdc_hist.unit_num AS unit_num,
                   cc_external_data_cdc_hist.pat_acct_num,
                   cc_external_data_cdc_hist.iplan_id,
                   cc_external_data_cdc_hist.iplan_order_num,
                   cc_external_data_cdc_hist.esl_level_1_desc,
                   cc_external_data_cdc_hist.esl_level_2_desc,
                   cc_external_data_cdc_hist.esl_level_3_desc,
                   cc_external_data_cdc_hist.esl_level_4_desc,
                   cc_external_data_cdc_hist.esl_level_5_desc,
                   cc_external_data_cdc_hist.chois_product_line_code AS chois_product_line_code,
                   cc_external_data_cdc_hist.chois_product_line_desc,
                   cc_external_data_cdc_hist.drg_medical_surgical_ind AS drg_medical_surgical_ind,
                   cc_external_data_cdc_hist.apr_drg_code AS apr_drg_code,
                   cc_external_data_cdc_hist.apr_drg_grouper_name AS apr_drg_grouper_name,
                   cc_external_data_cdc_hist.apr_severity_of_illness_desc AS apr_severity_of_illness_desc,
                   cc_external_data_cdc_hist.apr_risk_of_mortality_desc AS apr_risk_of_mortality_desc,
                   cc_external_data_cdc_hist.payer_type_code AS payer_type_code,
                   cc_external_data_cdc_hist.sub_payor_group_id AS sub_payor_group_id,
                   cc_external_data_cdc_hist.cond_code_xf_xg_ind,
                   cc_external_data_cdc_hist.cond_code_nu_ind,
                   cc_external_data_cdc_hist.cond_code_ne_ind,
                   cc_external_data_cdc_hist.cond_code_ns_ind,
                   cc_external_data_cdc_hist.cond_code_np_ind,
                   cc_external_data_cdc_hist.cond_code_no_ind,
                   cc_external_data_cdc_hist.treatment_authorization_num,
                   cc_external_data_cdc_hist.denial_in_midas_status,
                   cc_external_data_cdc_hist.midas_date_of_denial,
                   cc_external_data_cdc_hist.all_days_approved_ind,
                   cc_external_data_cdc_hist.ptp_performed,
                   cc_external_data_cdc_hist.cm_xf_ind,
                   cc_external_data_cdc_hist.cm_xg_ind,
                   cc_external_data_cdc_hist.cm_last_xf_code_applied_date,
                   cc_external_data_cdc_hist.cm_last_xg_code_applied_date,
                   cc_external_data_cdc_hist.midas_acct_num,
                   cc_external_data_cdc_hist.last_appeal_date,
                   cc_external_data_cdc_hist.last_appeal_status,
                   cc_external_data_cdc_hist.last_appeal_employee_id,
                   cc_external_data_cdc_hist.last_appeal_employee_name,
                   cc_external_data_cdc_hist.status_cause_name,
                   cc_external_data_cdc_hist.last_conc_review_disp,
                   cc_external_data_cdc_hist.midas_principal_payer_auth_num,
                   cc_external_data_cdc_hist.midas_principal_pyr_auth_type,
                   cc_external_data_cdc_hist.cm_last_iq_revi_crit_met_desc,
                   cc_external_data_cdc_hist.cm_last_iq_review_version_desc,
                   cc_external_data_cdc_hist.cm_last_iq_review_subset_desc,
                   cc_external_data_cdc_hist.pdu_determination_reason_desc,
                   cc_external_data_cdc_hist.ins1_payor_balance_amt,
                   cc_external_data_cdc_hist.ins2_payor_balance_amt,
                   cc_external_data_cdc_hist.ins3_payor_balance_amt,
                   cc_external_data_cdc_hist.first_doc_request_mr_date,
                   cc_external_data_cdc_hist.last_doc_request_mr_date,
                   cc_external_data_cdc_hist.first_doc_sent_mr_date,
                   cc_external_data_cdc_hist.last_doc_sent_mr_date,
                   cc_external_data_cdc_hist.first_doc_request_ib_date,
                   cc_external_data_cdc_hist.last_doc_request_ib_date,
                   cc_external_data_cdc_hist.first_doc_sent_ib_date,
                   cc_external_data_cdc_hist.last_doc_sent_ib_date,
                   cc_external_data_cdc_hist.first_doc_request_date,
                   cc_external_data_cdc_hist.last_doc_request_date,
                   cc_external_data_cdc_hist.first_doc_sent_date,
                   cc_external_data_cdc_hist.last_doc_sent_date,
                   cc_external_data_cdc_hist.first_doc_received_date,
                   cc_external_data_cdc_hist.last_doc_received_date,
                   cc_external_data_cdc_hist.first_doc_approved_date,
                   cc_external_data_cdc_hist.last_doc_approved_date,
                   cc_external_data_cdc_hist.first_doc_denied_date,
                   cc_external_data_cdc_hist.last_doc_denied_date,
                   cc_external_data_cdc_hist.covid_positive_flag,
                   cc_external_data_cdc_hist.refund_amt,
                   cc_external_data_cdc_hist.refund_create_date,
                   cc_external_data_cdc_hist.refund_requested_by,
                   cc_external_data_cdc_hist.patient_refund_amt,
                   cc_external_data_cdc_hist.patient_refund_create_date,
                   cc_external_data_cdc_hist.patient_refund_requested_by,
                   cc_external_data_cdc_hist.credit_status,
                   cc_external_data_cdc_hist.discrepancy_source_desc,
                   cc_external_data_cdc_hist.reimbursement_impact_desc,
                   cc_external_data_cdc_hist.discrepancy_date_time,
                   cc_external_data_cdc_hist.request_date_time,
                   cc_external_data_cdc_hist.reprocess_reason_text,
                   cc_external_data_cdc_hist.status_desc,
                   cc_external_data_cdc_hist.split_bill_ind,
                   cc_external_data_cdc_hist.last_scrted_appl_date_time,
                   cc_external_data_cdc_hist.scripted_overpayment_desc,
                   cc_external_data_cdc_hist.last_letter_sent_date_time,
                   cc_external_data_cdc_hist.account_id,
                   cc_external_data_cdc_hist.account_payor_id,
                   cc_external_data_cdc_hist.org_id,
                   cc_external_data_cdc_hist.first_ptp_completed_date,
                   cc_external_data_cdc_hist.first_strength_of_case,
                   cc_external_data_cdc_hist.last_ptp_completed_date,
                   cc_external_data_cdc_hist.last_strength_of_case,
                   cc_external_data_cdc_hist.total_ptp_closure_status_completed,
                   cc_external_data_cdc_hist.total_midnights,
                   cc_external_data_cdc_hist.total_inhouse_midnights,
                   cc_external_data_cdc_hist.gov_sec_tert_iplan,
                   cc_external_data_cdc_hist.takeback_follow_up_ltr_ind,
                   cc_external_data_cdc_hist.patient_refund_last_update_date_time,
                   cc_external_data_cdc_hist.payer_refund_last_update_date_time,
                   cc_external_data_cdc_hist.tmr_ma_ereq_auto_nonbillable_review_ind,
                   cc_external_data_cdc_hist.presentation_date,
                   cc_external_data_cdc_hist.artiva_claim_num,
                   cc_external_data_cdc_hist.recoup_date,
                   cc_external_data_cdc_hist.recoup_flag,
                   cc_external_data_cdc_hist.ground_transportation_ind,
                   cc_external_data_cdc_hist.dna_clinical_rationale_1,
                   cc_external_data_cdc_hist.dna_clinical_rationale_2,
                   cc_external_data_cdc_hist.dna_clinical_rationale_3,
                   cc_external_data_cdc_hist.dna_clinical_rationale_4,
                   cc_external_data_cdc_hist.dna_clinical_rationale_5,
                   cc_external_data_cdc_hist.dna_clinical_rationale_6,
                   cc_external_data_cdc_hist.dna_clinical_rationale_6_notes,
                   cc_external_data_cdc_hist.dna_ip_only_proc_waterpark_ind,
                   cc_external_data_cdc_hist.dna_ip_only_proc_user_ind,
                   cc_external_data_cdc_hist.psr_agree_disagree_flag,
                   cc_external_data_cdc_hist.first_psr_note_date_time,
                   cc_external_data_cdc_hist.last_psr_note_date_time,
                   cc_external_data_cdc_hist.dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist
   WHERE DATE(cc_external_data_cdc_hist.dw_last_update_date_time) = current_date('US/Central') ) AS ms ON upper(coalesce(mt.concuity_schema, '0')) = upper(coalesce(ms.concuity_schema, '0'))
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
AND (coalesce(mt.last_doc_request_date, DATE '1970-01-01') = coalesce(ms.last_doc_request_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_request_date, DATE '1970-01-02') = coalesce(ms.last_doc_request_date, DATE '1970-01-02'))
AND (coalesce(mt.first_doc_sent_date, DATE '1970-01-01') = coalesce(ms.first_doc_sent_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_sent_date, DATE '1970-01-02') = coalesce(ms.first_doc_sent_date, DATE '1970-01-02'))
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
AND (coalesce(cast(mt.discrepancy_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.discrepancy_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.discrepancy_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.discrepancy_date_time, timestamp '1970-01-01 00:00:01'))
AND (coalesce(cast(mt.request_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.request_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.request_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.request_date_time, timestamp '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.reprocess_reason_text, '0')) = upper(coalesce(ms.reprocess_reason_text, '0'))
     AND upper(coalesce(mt.reprocess_reason_text, '1')) = upper(coalesce(ms.reprocess_reason_text, '1')))
AND (upper(coalesce(mt.status_desc, '0')) = upper(coalesce(ms.status_desc, '0'))
     AND upper(coalesce(mt.status_desc, '1')) = upper(coalesce(ms.status_desc, '1')))
AND (upper(coalesce(mt.split_bill_ind, '0')) = upper(coalesce(ms.split_bill_ind, '0'))
     AND upper(coalesce(mt.split_bill_ind, '1')) = upper(coalesce(ms.split_bill_ind, '1')))
AND (coalesce(cast(mt.last_scrted_appl_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.last_scrted_appl_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_scrted_appl_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.last_scrted_appl_date_time, timestamp '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.scripted_overpayment_desc, '0')) = upper(coalesce(ms.scripted_overpayment_desc, '0'))
     AND upper(coalesce(mt.scripted_overpayment_desc, '1')) = upper(coalesce(ms.scripted_overpayment_desc, '1')))
AND (coalesce(cast(mt.last_letter_sent_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.last_letter_sent_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_letter_sent_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.last_letter_sent_date_time, timestamp '1970-01-01 00:00:01'))
AND (coalesce(mt.account_id, NUMERIC '0') = coalesce(ms.account_id, NUMERIC '0')
     AND coalesce(mt.account_id, NUMERIC '1') = coalesce(ms.account_id, NUMERIC '1'))
AND (coalesce(mt.account_payor_id, NUMERIC '0') = coalesce(ms.account_payor_id, NUMERIC '0')
     AND coalesce(mt.account_payor_id, NUMERIC '1') = coalesce(ms.account_payor_id, NUMERIC '1'))
AND (coalesce(mt.org_id, NUMERIC '0') = coalesce(ms.org_id, NUMERIC '0')
     AND coalesce(mt.org_id, NUMERIC '1') = coalesce(ms.org_id, NUMERIC '1'))
AND (coalesce(cast(mt.first_ptp_completed_date as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.first_ptp_completed_date, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.first_ptp_completed_date as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.first_ptp_completed_date, timestamp '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.first_strength_of_case, '0')) = upper(coalesce(ms.first_strength_of_case, '0'))
     AND upper(coalesce(mt.first_strength_of_case, '1')) = upper(coalesce(ms.first_strength_of_case, '1')))
AND (coalesce(cast(mt.last_ptp_completed_date as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.last_ptp_completed_date, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_ptp_completed_date as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.last_ptp_completed_date, timestamp '1970-01-01 00:00:01'))
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
AND (coalesce(cast(mt.patient_refund_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.patient_refund_last_update_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.patient_refund_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.patient_refund_last_update_date_time, timestamp '1970-01-01 00:00:01'))
AND (coalesce(cast(mt.payer_refund_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.payer_refund_last_update_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.payer_refund_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.payer_refund_last_update_date_time, timestamp '1970-01-01 00:00:01'))
AND (coalesce(cast(mt.dw_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, timestamp '1970-01-01 00:00:00')
     AND coalesce(cast(mt.dw_last_update_date_time as timestamp), timestamp '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, timestamp '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
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
        last_psr_note_date_time,
        dw_last_update_date_time)
VALUES (ms.concuity_schema, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.iplan_order_num, ms.esl_level_1_desc, ms.esl_level_2_desc, ms.esl_level_3_desc, ms.esl_level_4_desc, ms.esl_level_5_desc, ms.chois_product_line_code, ms.chois_product_line_desc, ms.drg_medical_surgical_ind, ms.apr_drg_code, ms.apr_drg_grouper_name, ms.apr_severity_of_illness_desc, ms.apr_risk_of_mortality_desc, ms.payer_type_code, ms.sub_payor_group_id, ms.cond_code_xf_xg_ind, ms.cond_code_nu_ind, ms.cond_code_ne_ind, ms.cond_code_ns_ind, ms.cond_code_np_ind, ms.cond_code_no_ind, ms.treatment_authorization_num, ms.denial_in_midas_status, ms.midas_date_of_denial, ms.all_days_approved_ind, ms.ptp_performed, ms.cm_xf_ind, ms.cm_xg_ind, ms.cm_last_xf_code_applied_date, ms.cm_last_xg_code_applied_date, ms.midas_acct_num, ms.last_appeal_date, ms.last_appeal_status, ms.last_appeal_employee_id, ms.last_appeal_employee_name, ms.status_cause_name, ms.last_conc_review_disp, ms.midas_principal_payer_auth_num, ms.midas_principal_pyr_auth_type, ms.cm_last_iq_revi_crit_met_desc, ms.cm_last_iq_review_version_desc, ms.cm_last_iq_review_subset_desc, ms.pdu_determination_reason_desc, ms.ins1_payor_balance_amt, ms.ins2_payor_balance_amt, ms.ins3_payor_balance_amt, ms.first_doc_request_mr_date, ms.last_doc_request_mr_date, ms.first_doc_sent_mr_date, ms.last_doc_sent_mr_date, ms.first_doc_request_ib_date, ms.last_doc_request_ib_date, ms.first_doc_sent_ib_date, ms.last_doc_sent_ib_date, ms.first_doc_request_date, ms.last_doc_request_date, ms.first_doc_sent_date, ms.last_doc_sent_date, ms.first_doc_received_date, ms.last_doc_received_date, ms.first_doc_approved_date, ms.last_doc_approved_date, ms.first_doc_denied_date, ms.last_doc_denied_date, ms.covid_positive_flag, ms.refund_amt, ms.refund_create_date, ms.refund_requested_by, ms.patient_refund_amt, ms.patient_refund_create_date, ms.patient_refund_requested_by, ms.credit_status, ms.discrepancy_source_desc, ms.reimbursement_impact_desc, cast(ms.discrepancy_date_time as datetime), cast(ms.request_date_time as datetime), ms.reprocess_reason_text, ms.status_desc, ms.split_bill_ind, cast(ms.last_scrted_appl_date_time as datetime), ms.scripted_overpayment_desc, cast(ms.last_letter_sent_date_time as datetime), ms.account_id, ms.account_payor_id, ms.org_id, cast(ms.first_ptp_completed_date as datetime), ms.first_strength_of_case, cast(ms.last_ptp_completed_date as datetime), ms.last_strength_of_case, ms.total_ptp_closure_status_completed, ms.total_midnights, cast(ms.total_inhouse_midnights as int64), ms.gov_sec_tert_iplan, ms.takeback_follow_up_ltr_ind, cast(ms.patient_refund_last_update_date_time as datetime), cast(ms.payer_refund_last_update_date_time as datetime),  ms.tmr_ma_ereq_auto_nonbillable_review_ind, ms.presentation_date, ms.artiva_claim_num, ms.recoup_date,ms.recoup_flag, ms.ground_transportation_ind, ms.dna_clinical_rationale_1, ms.dna_clinical_rationale_2, ms.dna_clinical_rationale_3, ms.dna_clinical_rationale_4, ms.dna_clinical_rationale_5, ms.dna_clinical_rationale_6, ms.dna_clinical_rationale_6_notes, ms.dna_ip_only_proc_waterpark_ind, ms.dna_ip_only_proc_user_ind, ms.psr_agree_disagree_flag, ms.first_psr_note_date_time, ms.last_psr_note_date_time, cast(ms.dw_last_update_date_time as datetime));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra_staging','CC_External_Data_CDC');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_external_data_cdc_hist AS mt USING
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
                   substr(vtl.ptp_performed, 1, 34) AS ptp_performed,
                   substr(vtl.all_days_approved_ind, 1, 2) AS all_days_approved_ind,
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
                   CAST(vtl.total_inhouse_midnights AS INT64) AS total_inhouse_midnights,
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
                   vtl.last_psr_note_date_time,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
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
AND (upper(coalesce(mt.ptp_performed, '0')) = upper(coalesce(ms.ptp_performed, '0'))
     AND upper(coalesce(mt.ptp_performed, '1')) = upper(coalesce(ms.ptp_performed, '1')))
AND (upper(coalesce(mt.all_days_approved_ind, '0')) = upper(coalesce(ms.all_days_approved_ind, '0'))
     AND upper(coalesce(mt.all_days_approved_ind, '1')) = upper(coalesce(ms.all_days_approved_ind, '1')))
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
AND (coalesce(mt.first_doc_sent_date, DATE '1970-01-01') = coalesce(ms.first_doc_sent_date, DATE '1970-01-01')
     AND coalesce(mt.first_doc_sent_date, DATE '1970-01-02') = coalesce(ms.first_doc_sent_date, DATE '1970-01-02'))
AND (coalesce(mt.last_doc_request_date, DATE '1970-01-01') = coalesce(ms.last_doc_request_date, DATE '1970-01-01')
     AND coalesce(mt.last_doc_request_date, DATE '1970-01-02') = coalesce(ms.last_doc_request_date, DATE '1970-01-02'))
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
AND (coalesce(cast(mt.discrepancy_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.discrepancy_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.discrepancy_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.discrepancy_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (coalesce(cast(mt.request_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.request_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.request_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.request_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.reprocess_reason_text, '0')) = upper(coalesce(ms.reprocess_reason_text, '0'))
     AND upper(coalesce(mt.reprocess_reason_text, '1')) = upper(coalesce(ms.reprocess_reason_text, '1')))
AND (upper(coalesce(mt.status_desc, '0')) = upper(coalesce(ms.status_desc, '0'))
     AND upper(coalesce(mt.status_desc, '1')) = upper(coalesce(ms.status_desc, '1')))
AND (upper(coalesce(mt.split_bill_ind, '0')) = upper(coalesce(ms.split_bill_ind, '0'))
     AND upper(coalesce(mt.split_bill_ind, '1')) = upper(coalesce(ms.split_bill_ind, '1')))
AND (coalesce(cast(mt.last_scrted_appl_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.last_scrted_appl_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_scrted_appl_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.last_scrted_appl_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.scripted_overpayment_desc, '0')) = upper(coalesce(ms.scripted_overpayment_desc, '0'))
     AND upper(coalesce(mt.scripted_overpayment_desc, '1')) = upper(coalesce(ms.scripted_overpayment_desc, '1')))
AND (coalesce(cast(mt.last_letter_sent_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.last_letter_sent_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_letter_sent_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.last_letter_sent_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.account_id, NUMERIC '0') = coalesce(ms.account_id, NUMERIC '0')
     AND coalesce(mt.account_id, NUMERIC '1') = coalesce(ms.account_id, NUMERIC '1'))
AND (coalesce(mt.account_payor_id, NUMERIC '0') = coalesce(ms.account_payor_id, NUMERIC '0')
     AND coalesce(mt.account_payor_id, NUMERIC '1') = coalesce(ms.account_payor_id, NUMERIC '1'))
AND (coalesce(mt.org_id, NUMERIC '0') = coalesce(ms.org_id, NUMERIC '0')
     AND coalesce(mt.org_id, NUMERIC '1') = coalesce(ms.org_id, NUMERIC '1'))
AND (coalesce(cast(mt.first_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.first_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.first_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.first_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.first_strength_of_case, '0')) = upper(coalesce(ms.first_strength_of_case, '0'))
     AND upper(coalesce(mt.first_strength_of_case, '1')) = upper(coalesce(ms.first_strength_of_case, '1')))
AND (coalesce(cast(mt.last_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.last_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.last_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.last_ptp_completed_date as datetime), DATETIME '1970-01-01 00:00:01'))
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
AND (coalesce(cast(mt.patient_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.patient_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.patient_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.patient_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (coalesce(cast(mt.payer_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(cast(ms.payer_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.payer_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(cast(ms.payer_refund_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:01'))
AND (mt.tmr_ma_ereq_auto_nonbillable_review_ind = ms.tmr_ma_ereq_auto_nonbillable_review_ind
     OR mt.tmr_ma_ereq_auto_nonbillable_review_ind IS NULL
     AND ms.tmr_ma_ereq_auto_nonbillable_review_ind IS NULL)
AND (mt.presentation_date = ms.presentation_date
     OR mt.presentation_date IS NULL
     AND ms.presentation_date IS NULL)
AND (mt.artiva_claim_num = ms.artiva_claim_num
     OR mt.artiva_claim_num IS NULL
     AND ms.artiva_claim_num IS NULL)
AND (mt.recoup_date = ms.recoup_date
     OR mt.recoup_date IS NULL
     AND ms.recoup_date IS NULL)
AND (mt.recoup_flag = ms.recoup_flag
     OR mt.recoup_flag IS NULL
     AND ms.recoup_flag IS NULL)
AND (mt.ground_transportation_ind = ms.ground_transportation_ind
     OR mt.ground_transportation_ind IS NULL
     AND ms.ground_transportation_ind IS NULL)
AND (mt.dna_clinical_rationale_1 = ms.dna_clinical_rationale_1
     OR mt.dna_clinical_rationale_1 IS NULL
     AND ms.dna_clinical_rationale_1 IS NULL)
AND (mt.dna_clinical_rationale_2 = ms.dna_clinical_rationale_2
     OR mt.dna_clinical_rationale_2 IS NULL
     AND ms.dna_clinical_rationale_2 IS NULL)
AND (mt.dna_clinical_rationale_3 = ms.dna_clinical_rationale_3
     OR mt.dna_clinical_rationale_3 IS NULL
     AND ms.dna_clinical_rationale_3 IS NULL)
AND (mt.dna_clinical_rationale_4 = ms.dna_clinical_rationale_4
     OR mt.dna_clinical_rationale_4 IS NULL
     AND ms.dna_clinical_rationale_4 IS NULL)
AND (mt.dna_clinical_rationale_5 = ms.dna_clinical_rationale_5
     OR mt.dna_clinical_rationale_5 IS NULL
     AND ms.dna_clinical_rationale_5 IS NULL)
AND (mt.dna_clinical_rationale_6 = ms.dna_clinical_rationale_6
     OR mt.dna_clinical_rationale_6 IS NULL
     AND ms.dna_clinical_rationale_6 IS NULL)
AND (mt.dna_clinical_rationale_6_notes = ms.dna_clinical_rationale_6_notes
     OR mt.dna_clinical_rationale_6_notes IS NULL
     AND ms.dna_clinical_rationale_6_notes IS NULL)
AND (mt.dna_ip_only_proc_waterpark_ind = ms.dna_ip_only_proc_waterpark_ind
     OR mt.dna_ip_only_proc_waterpark_ind IS NULL
     AND ms.dna_ip_only_proc_waterpark_ind IS NULL)
AND (mt.dna_ip_only_proc_user_ind = ms.dna_ip_only_proc_user_ind
     OR mt.dna_ip_only_proc_user_ind IS NULL
     AND ms.dna_ip_only_proc_user_ind IS NULL)
AND (mt.psr_agree_disagree_flag = ms.psr_agree_disagree_flag
     OR mt.psr_agree_disagree_flag IS NULL
     AND ms.psr_agree_disagree_flag IS NULL)
AND (cast(mt.first_psr_note_date_time as datetime) = cast(ms.first_psr_note_date_time as datetime)
     OR mt.first_psr_note_date_time IS NULL
     AND ms.first_psr_note_date_time IS NULL)
AND (cast(mt.last_psr_note_date_time as datetime) = cast(ms.last_psr_note_date_time as datetime)
     OR mt.last_psr_note_date_time IS NULL
     AND ms.last_psr_note_date_time IS NULL)
AND (coalesce(cast(mt.dw_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(cast(mt.dw_last_update_date_time as datetime), DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
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
        ptp_performed,
        all_days_approved_ind,
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
        first_doc_sent_date,
        last_doc_request_date,
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
        last_psr_note_date_time,
        dw_last_update_date_time)
VALUES (ms.concuity_schema, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.iplan_order_num, ms.esl_level_1_desc, ms.esl_level_2_desc, ms.esl_level_3_desc, ms.esl_level_4_desc, ms.esl_level_5_desc, ms.chois_product_line_code, ms.chois_product_line_desc, ms.drg_medical_surgical_ind, ms.apr_drg_code, ms.apr_drg_grouper_name, ms.apr_severity_of_illness_desc, ms.apr_risk_of_mortality_desc, ms.payer_type_code, ms.sub_payor_group_id, ms.cond_code_xf_xg_ind, ms.cond_code_nu_ind, ms.cond_code_ne_ind, ms.cond_code_ns_ind, ms.cond_code_np_ind, ms.cond_code_no_ind, ms.treatment_authorization_num, ms.denial_in_midas_status, ms.midas_date_of_denial, ms.ptp_performed, ms.all_days_approved_ind, ms.cm_xf_ind, ms.cm_xg_ind, ms.cm_last_xf_code_applied_date, ms.cm_last_xg_code_applied_date, ms.midas_acct_num, ms.last_appeal_date, ms.last_appeal_status, ms.last_appeal_employee_id, ms.last_appeal_employee_name, ms.status_cause_name, ms.last_conc_review_disp, ms.midas_principal_payer_auth_num, ms.midas_principal_pyr_auth_type, ms.cm_last_iq_revi_crit_met_desc, ms.cm_last_iq_review_version_desc, ms.cm_last_iq_review_subset_desc, ms.pdu_determination_reason_desc, ms.ins1_payor_balance_amt, ms.ins2_payor_balance_amt, ms.ins3_payor_balance_amt, ms.first_doc_request_mr_date, ms.last_doc_request_mr_date, ms.first_doc_sent_mr_date, ms.last_doc_sent_mr_date, ms.first_doc_request_ib_date, ms.last_doc_request_ib_date, ms.first_doc_sent_ib_date, ms.last_doc_sent_ib_date, ms.first_doc_request_date, ms.first_doc_sent_date, ms.last_doc_request_date, ms.last_doc_sent_date, ms.first_doc_received_date, ms.last_doc_received_date, ms.first_doc_approved_date, ms.last_doc_approved_date, ms.first_doc_denied_date, ms.last_doc_denied_date, ms.covid_positive_flag, ms.refund_amt, ms.refund_create_date, ms.refund_requested_by, ms.patient_refund_amt, ms.patient_refund_create_date, ms.patient_refund_requested_by, ms.credit_status, ms.discrepancy_source_desc, ms.reimbursement_impact_desc, cast(ms.discrepancy_date_time as timestamp), cast(ms.request_date_time as timestamp), ms.reprocess_reason_text, ms.status_desc, ms.split_bill_ind, cast(ms.last_scrted_appl_date_time as timestamp), ms.scripted_overpayment_desc, cast(ms.last_letter_sent_date_time as timestamp), ms.account_id, ms.account_payor_id, ms.org_id, cast(ms.first_ptp_completed_date as timestamp), ms.first_strength_of_case, cast(ms.last_ptp_completed_date as timestamp), ms.last_strength_of_case, ms.total_ptp_closure_status_completed, ms.total_midnights, cast(ms.total_inhouse_midnights as INT64), ms.gov_sec_tert_iplan, ms.takeback_follow_up_ltr_ind, cast(ms.patient_refund_last_update_date_time as timestamp), cast(ms.payer_refund_last_update_date_time as timestamp), ms.tmr_ma_ereq_auto_nonbillable_review_ind, ms.presentation_date, ms.artiva_claim_num,ms.recoup_date, ms.recoup_flag, ms.ground_transportation_ind, ms.dna_clinical_rationale_1, ms.dna_clinical_rationale_2, ms.dna_clinical_rationale_3, ms.dna_clinical_rationale_4, ms.dna_clinical_rationale_5, ms.dna_clinical_rationale_6, ms.dna_clinical_rationale_6_notes, ms.dna_ip_only_proc_waterpark_ind, ms.dna_ip_only_proc_user_ind, ms.psr_agree_disagree_flag, cast(ms.first_psr_note_date_time as timestamp), cast(ms.last_psr_note_date_time as timestamp), cast(ms.dw_last_update_date_time as timestamp));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra_staging','CC_External_Data_CDC_Hist');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;