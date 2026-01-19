CREATE OR REPLACE VIEW {{ params.param_parallon_pbs_views_dataset_name }}.edr_export AS
SELECT org.concuity_schema AS concuity_schema,
       ccap.unit_num AS unit_num,
       ccap.pat_acct_num AS pat_acct_num,
       ccap.iplan_id AS iplan_id,
       ccap.iplan_order_num AS iplan_order_num,
       esl.esl_level_1_desc AS esl_level_1_desc,
       esl.esl_level_2_desc AS esl_level_2_desc,
       esl.esl_level_3_desc AS esl_level_3_desc,
       esl.esl_level_4_desc AS esl_level_4_desc,
       esl.esl_level_5_desc AS esl_level_5_desc,
       drg_pa.chois_product_line_code AS chois_product_line_code,
       drg_pa.chois_product_line_desc AS chois_product_line_desc,
       drg_pa.drg_medical_surgical_ind AS drg_medical_surgical_ind,
       apr.apr_drg_code AS apr_drg_code,
       apr.apr_drg_grouper_name AS apr_drg_grouper_name,
       apr.apr_severity_of_illness_desc AS apr_severity_of_illness_desc,
       apr.apr_risk_of_mortality_desc AS apr_risk_of_mortality_desc,
       fi.payer_type_code AS payer_type_code,
       fi.sub_payor_group_id AS sub_payor_group_id,
       coalesce(condcode.cond_code_xf_xg_ind, 'N') AS cond_code_xf_xg_ind,
       coalesce(condcode.cond_code_nu_ind, 'N') AS cond_code_nu_ind,
       coalesce(condcode.cond_code_ne_ind, 'N') AS cond_code_ne_ind,
       coalesce(condcode.cond_code_ns_ind, 'N') AS cond_code_ns_ind,
       coalesce(condcode.cond_code_np_ind, 'N') AS cond_code_np_ind,
       coalesce(condcode.cond_code_no_ind, 'N') AS cond_code_no_ind,
       ri.treatment_authorization_num AS treatment_authorization_num, -- CMD.Denial_in_Midas_Status AS Denial_in_Midas_Status,
 CASE 1
     WHEN urs.acct_denial_rcvd_concurrent_sw THEN 'Concurrent Denial (Pre Disch)'
     WHEN urs.acct_denial_rcvd_post_disch_sw THEN 'Denial in Midas (Post Disch)'
     WHEN urs.acct_with_denial_date_not_doc_sw THEN 'Denial in Midas (No DNL Date)'
     ELSE 'Not in Midas'
 END AS denial_in_midas_status,
 cmd.midas_date_of_denial AS midas_date_of_denial, -- Coalesce(CMD.All_Days_Approved_Ind, 'N') AS All_Days_Approved_Ind,
 coalesce(CASE
              WHEN urs.all_day_authr_sw = 1 THEN 'Y'
              ELSE 'N'
          END, 'N') AS all_days_approved_ind,
 urs.ptp_performed_ind AS ptp_performed,
 coalesce(cmd.cm_xf_ind, 'N') AS cm_xf_ind,
 coalesce(cmd.cm_xg_ind, 'N') AS cm_xg_ind,
 cmd.cm_last_xf_code_applied_date AS cm_last_xf_code_applied_date,
 cmd.cm_last_xg_code_applied_date AS cm_last_xg_code_applied_date,
 cmd.midas_acct_num AS midas_acct_num,
 cmd.last_appeal_date AS last_appeal_date,
 cmd.last_appeal_status AS last_appeal_status,
 cmd.last_appeal_employee_id AS last_appeal_employee_id,
 cmd.last_appeal_employee_name AS last_appeal_employee_name,
 cmd.status_cause_name AS status_cause_name,
 cmd.last_conc_review_disp AS last_conc_review_disp,
 cmd.midas_principal_payer_auth_num AS midas_principal_payer_auth_num,
 cmd.midas_principal_payer_auth_type AS midas_principal_pyr_auth_type,
 cmd.cm_last_iq_review_criteria_met_desc AS cm_last_iq_revi_crit_met_desc,
 cmd.cm_last_iq_review_version_desc AS cm_last_iq_review_version_desc,
 cmd.cm_last_iq_review_subset_desc AS cm_last_iq_review_subset_desc,
 pdu.pdu_determination_reason_desc AS pdu_determination_reason_desc,
 pa.ins1_payor_balance_amt AS ins1_payor_balance_amt,
 pa.ins2_payor_balance_amt AS ins2_payor_balance_amt,
 pa.ins3_payor_balance_amt AS ins3_payor_balance_amt, -- PREPAY.Doc_Request_Type,
 prepay.min_doc_req_medrec_request_date AS first_doc_request_mr_date,
 prepay.max_doc_req_medrec_request_date AS last_doc_request_mr_date,
 prepay.min_doc_req_medrec_sent_date AS first_doc_sent_mr_date,
 prepay.max_doc_req_medrec_sent_date AS last_doc_sent_mr_date,
 prepay.min_doc_req_ib_request_date AS first_doc_request_ib_date,
 prepay.max_doc_req_ib_request_date AS last_doc_request_ib_date,
 prepay.min_doc_req_ib_sent_date AS first_doc_sent_ib_date,
 prepay.max_doc_req_ib_sent_date AS last_doc_sent_ib_date,
 prepay.first_doc_request_date AS first_doc_request_date,
 prepay.last_doc_request_date AS last_doc_request_date,
 prepay.first_doc_sent_date AS first_doc_sent_date,
 prepay.last_doc_sent_date AS last_doc_sent_date,
 prepay.first_doc_received_date AS first_doc_received_date,
 prepay.last_doc_received_date AS last_doc_received_date,
 prepay.first_doc_approved_date AS first_doc_approved_date,
 prepay.last_doc_approved_date AS last_doc_approved_date,
 prepay.first_doc_denied_date AS first_doc_denied_date,
 prepay.last_doc_denied_date AS last_doc_denied_date,
 coalesce(CASE
              WHEN cov.patient_dw_id IS NOT NULL THEN 'Y'
              ELSE 'N'
          END, 'N') AS covid_positive_flag,
 cr1py.refund_amt AS refund_amt,
 cr1py.entered_date AS refund_create_date,
 cr1py.refund_creation_user_id AS refund_requested_by,
 crb.last_update_date_time AS payer_refund_last_update_date_time,
 cr1pt.refund_amt AS patient_refund_amt,
 cr1pt.entered_date AS patient_refund_create_date,
 cr1pt.refund_creation_user_id AS patient_refund_requested_by,
 cr1pt.last_update_date_time AS patient_refund_last_update_date_time,
 crb.credit_status_alias AS credit_status,
 msccr.discrepancy_source_desc AS discrepancy_source_desc,
 msccr.reimbursement_impact_desc AS reimbursement_impact_desc,
 msccr.discrepancy_date_time AS discrepancy_date_time,
 msccr.request_date_time AS request_date_time,
 msccr.reprocess_reason_text AS reprocess_reason_text,
 msccr.status_desc AS status_desc,
 coalesce(splitbill.split_bill_ind, 'N') AS split_bill_ind,
 scr.last_scripted_applied_date_time AS last_scrted_appl_date_time,
 scr.scripted_overpayment_desc AS scripted_overpayment_desc,
 ltr.last_letter_sent_date_time AS last_letter_sent_date_time,
 cca.account_id AS account_id,
 ccap.account_payor_id AS account_payor_id,
 ccorg.org_id AS org_id,
 gov_sec_tert_iplan AS gov_sec_tert_iplan,
 CASE
     WHEN ad.care_start_date <> DATE '0001-01-01'
          AND ad.care_start_date IS NOT NULL
          AND ad.care_start_date < fp.admission_date THEN date_diff(fp.discharge_date, ad.care_start_date, DAY)
     ELSE date_diff(fp.discharge_date, fp.admission_date, DAY)
 END AS total_midnights,
 fp.total_midnights_in_house_amt AS total_inhouse_midnights,
 CASE
     WHEN upper(rtrim(crb.credit_status_alias)) = 'TB - TAKE BACK'
          AND upper(rtrim(fp.company_code)) = 'H'
          AND upper(rtrim(gov_sec_tert_iplan)) = 'N'
          AND ccap.iplan_order_num = 1
          AND CAST({{ params.param_bqutil_fns_dataset_name }}.cw_td_normalize_number(fi.major_payor_group_id) AS FLOAT64) NOT IN(CAST(555 AS FLOAT64),
                                                                                                                            CAST(923 AS FLOAT64))
          AND upper(rtrim(org.consolidated_ssc_alias_name)) NOT IN('RICHMOND SSC')
          AND date_diff(current_date('US/Central'), DATE(crb.last_update_date_time), DAY) > 26 THEN 'Y'
     ELSE 'N'
 END AS takeback_follow_up_ltr_ind,
 tmr_ptp.first_ptp_completed_date,
 tmr_ptp.first_strength_of_case,
 tmr_ptp.last_ptp_completed_date,
 tmr_ptp.last_strength_of_case,
 tmr_ptp.total_ptp_closure_status_completed,
 CASE
     WHEN erequest.patient_dw_id IS NOT NULL THEN 'Y'
     ELSE 'N'
 END AS tmr_ma_ereq_auto_nonbillable_review_ind,
 CASE
     WHEN ad.care_start_date <> DATE '0001-01-01'
          AND ad.care_start_date IS NOT NULL
          AND ad.care_start_date < fp.admission_date THEN ad.care_start_date
     ELSE fp.admission_date
 END AS presentation_date,
 coalesce(artiva_cl_num.artiva_claim_num, '') AS artiva_claim_num,
 CASE
     WHEN dnaclm.clin_rat_1 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_1,
 CASE
     WHEN dnaclm.clin_rat_2 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_2,
 CASE
     WHEN dnaclm.clin_rat_3 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_3,
 CASE
     WHEN dnaclm.clin_rat_4 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_4,
 CASE
     WHEN dnaclm.clin_rat_5 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_5,
 CASE
     WHEN dnaclm.clin_rat_6 = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_clinical_rationale_6,
 dnaclm.clin_rat_6_comm AS dna_clinical_rationale_6_notes,
 CASE
     WHEN dnaclm.inpatient_only_procedure_waterpark_sw = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_ip_only_proc_waterpark_ind,
 CASE
     WHEN dnaclm.inpatient_only_procedure_user_sw = 1 THEN 'Y'
     ELSE 'N'
 END AS dna_ip_only_proc_user_ind,
 CASE
     WHEN rtrim(hdfm.agree_flag) = 'Y' THEN 'Agree'
     WHEN rtrim(hdfm.disagree_flag) = 'Y'
          AND rtrim(hdfm.agree_flag) = 'N' THEN 'Disagree'
     WHEN rtrim(hdfm.agree_flag) = 'N'
          AND rtrim(hdfm.disagree_flag) = 'N' THEN 'No Valid eRequest Note'
     ELSE 'No PSR eRequest Found'
 END AS psr_agree_disagree_flag,
 hdfm.first_note_date_time AS first_psr_note_date_time,
 hdfm.last_note_date_time AS last_psr_note_date_time
FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS ccap
LEFT OUTER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account AS cca ON cca.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS ccorg ON upper(rtrim(ccorg.company_code)) = upper(rtrim(ccap.company_code))
AND upper(rtrim(ccorg.coid)) = upper(rtrim(ccap.coid))
INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp ON fp.patient_dw_id = ccap.patient_dw_id
AND upper(rtrim(fp.company_code)) = upper(rtrim(ccap.company_code))
AND upper(rtrim(fp.coid)) = upper(rtrim(ccap.coid))
INNER JOIN
  (SELECT o.company_code,
          o.coid
   FROM {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS o
   WHERE upper(rtrim(o.active_ind)) = 'Y' ) AS f_active ON upper(rtrim(f_active.coid)) = upper(rtrim(fp.coid))
AND upper(rtrim(f_active.company_code)) = upper(rtrim(fp.company_code))
LEFT OUTER JOIN
  (SELECT ei.pat_acct_num,
          ei.coid,
          ei.patient_dw_id,
          ei.payor_dw_id,
          ei.iplan_id
   FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.erequest_inventory AS ei
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_ssi_responsible_dept AS dref ON ei.current_queue_dept_id = dref.ssi_queue_dept_id
   WHERE upper(rtrim(dref.ssi_queue_dept_desc)) = 'NET AR MA AUTOMATION REVIEW'
     AND upper(rtrim(ei.unbilled_reason_code)) = 'HDUO'
     AND ei.rptg_date =
       (SELECT -- AND Request_Status_Code = 'O' I think everything in the table is open
 max(erequest_inventory.rptg_date)
        FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.erequest_inventory) QUALIFY row_number() OVER (PARTITION BY ei.pat_acct_num,
                                                                                                                 upper(ei.coid),
                                                                                                                 ei.patient_dw_id,
                                                                                                                 ei.payor_dw_id,
                                                                                                                 ei.iplan_id
                                                                                                    ORDER BY ei.rptg_date DESC) = 1 ) AS erequest ON erequest.patient_dw_id = ccap.patient_dw_id
AND erequest.payor_dw_id = ccap.payor_dw_id
LEFT OUTER JOIN --  Only Open today

  (SELECT collection_encounter_detail.patient_dw_id,
          collection_encounter_detail.payor_dw_id,
          collection_encounter_detail.liability_sequence_number,
          collection_encounter_detail.claim_num AS artiva_claim_num
   FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.collection_encounter_detail
   WHERE collection_encounter_detail.dw_last_update_date_time =
       (SELECT max(collection_encounter_detail_0.dw_last_update_date_time)
        FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.collection_encounter_detail AS collection_encounter_detail_0)
     AND upper(rtrim(collection_encounter_detail.artiva_instance_code)) = 'AR1'
     AND collection_encounter_detail.liability_sequence_number IN(1,
                                                                  2,
                                                                  3) QUALIFY row_number() OVER (PARTITION BY collection_encounter_detail.patient_dw_id,
                                                                                                             collection_encounter_detail.payor_dw_id,
                                                                                                             collection_encounter_detail.liability_sequence_number,
                                                                                                             upper(artiva_claim_num)
                                                                                                ORDER BY collection_encounter_detail.dw_last_update_date_time DESC) = 1 ) AS artiva_cl_num ON artiva_cl_num.patient_dw_id = ccap.patient_dw_id
AND artiva_cl_num.payor_dw_id = ccap.payor_dw_id
AND artiva_cl_num.liability_sequence_number = ccap.iplan_order_num
LEFT OUTER JOIN
  (SELECT er.patient_dw_id,
          max(CASE
                  WHEN upper(er.notes_desc) LIKE '%AGREE%'
                       AND upper(er.notes_desc) NOT LIKE '%DISAGREE%' THEN 'Y'
                  ELSE 'N'
              END) AS agree_flag,
          max(CASE
                  WHEN upper(er.notes_desc) LIKE '%DISAGREE%' THEN 'Y'
                  ELSE 'N'
              END) AS disagree_flag,
          min(er.note_date_time) AS first_note_date_time,
          max(er.note_date_time) AS last_note_date_time
   FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.erequest_productivity_dtl AS er
   WHERE upper(rtrim(er.unbilled_reason_code)) = 'HDFM'
   GROUP BY 1) AS hdfm ON ccap.patient_dw_id = hdfm.patient_dw_id
LEFT OUTER JOIN
  (SELECT DISTINCT drv_strengthofcase.patient_dw_id, -- P2PCompletedDate
 max(CASE
         WHEN drv_strengthofcase.firststrengthofcaserank = 1 THEN drv_strengthofcase.p2pcompleteddate
     END) AS first_ptp_completed_date,
 max(CASE
         WHEN drv_strengthofcase.firststrengthofcaserank = 1 THEN drv_strengthofcase.closure_status_desc
     END) AS first_strength_of_case,
 max(CASE
         WHEN drv_strengthofcase.laststrengthofcaserank = 1 THEN drv_strengthofcase.p2pcompleteddate
     END) AS last_ptp_completed_date,
 max(CASE
         WHEN drv_strengthofcase.laststrengthofcaserank = 1 THEN drv_strengthofcase.closure_status_desc
     END) AS last_strength_of_case,
 count(drv_strengthofcase.claim_id) AS total_ptp_closure_status_completed
   FROM
     (SELECT clm.patient_dw_id, -- Clm.Pat_Acct_Num,
 -- Clm.Closure_status_id ,
 ref_cm_dna_closure_status.closure_status_desc,
 clm.peer_to_peer_completed_date_time_utc AS p2pcompleteddate,
 clm.claim_id,
 rank() OVER (PARTITION BY clm.patient_dw_id
              ORDER BY clm.peer_to_peer_completed_date_time_utc) AS firststrengthofcaserank,
             rank() OVER (PARTITION BY clm.patient_dw_id
                          ORDER BY clm.peer_to_peer_completed_date_time_utc DESC) AS laststrengthofcaserank
      FROM {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fctptnt
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ftfacility ON upper(rtrim(fctptnt.coid)) = upper(rtrim(ftfacility.coid))
      AND upper(rtrim(fctptnt.company_code)) = upper(rtrim(ftfacility.company_code))
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_dna_claim AS clm ON fctptnt.patient_dw_id = clm.patient_dw_id
      AND clm.eff_to_date_time = DATETIME '9999-12-31 00:00:00'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_cm_dna_closure_status ON clm.closure_status_id = ref_cm_dna_closure_status.closure_status_id
      AND ref_cm_dna_closure_status.eff_to_date_time = DATETIME '9999-12-31 00:00:00'
      WHERE upper(rtrim(ftfacility.company_code)) = 'H'
        AND upper(rtrim(ftfacility.coid_status_code)) = 'F'
        AND upper(fctptnt.admission_patient_type_code) LIKE '%I%'
        AND clm.claim_status_id = 6
        AND clm.peer_to_peer_status_id = 4
        AND clm.closure_status_id IS NOT NULL ) AS drv_strengthofcase
   WHERE drv_strengthofcase.firststrengthofcaserank = 1
     OR drv_strengthofcase.laststrengthofcaserank = 1
   GROUP BY 1) AS tmr_ptp ON tmr_ptp.patient_dw_id = fp.patient_dw_id
LEFT OUTER JOIN --  comlpeted claims
 --  P2P Completed
 -- AND Clm.Patient_Dw_Id in(233429908001848834)
 -- AND FctPtnt.Discharge_Date BETWEEN TRUNC(Date, 'Y') - Interval '3' YEAR AND Add_Months(Trunc(Date, 'Month'),-1)-1
 --  bring in accounts/claims with strength of case captured
 {{ params.param_parallon_pbs_views_dataset_name }}.patient_account_detail_lvl AS pa ON pa.patient_dw_id = ccap.patient_dw_id
INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS fi ON fi.payor_dw_id = ccap.payor_dw_id
AND upper(rtrim(fi.company_code)) = upper(rtrim(ccap.company_code))
LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.patient_esl_detail AS esl ON esl.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN {{ params.param_parallon_pbs_views_dataset_name }}.dim_organization AS org ON upper(rtrim(org.coid)) = upper(rtrim(ccap.coid))
AND upper(rtrim(org.company_code)) = upper(rtrim(ccap.company_code))
LEFT OUTER JOIN
  (SELECT DISTINCT ccap_0.patient_dw_id
   FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS ccap_0
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS fi_0 ON fi_0.payor_dw_id = ccap_0.payor_dw_id
   AND upper(rtrim(fi_0.company_code)) = upper(rtrim(ccap_0.company_code))
   WHERE ccap_0.iplan_order_num > 1
     AND CAST(fi_0.iplan_financial_class_code AS INT64) IN(1,
                                                           2,
                                                           3,
                                                           6) ) AS gov_flag ON gov_flag.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.admission AS ad ON ad.patient_dw_id = fp.patient_dw_id
AND upper(rtrim(ad.company_code)) = upper(rtrim(fp.company_code))
LEFT OUTER JOIN
  (SELECT dg.reimbursement_group_code,
          dg.reimbursement_group_start_date,
          dg.reimbursement_group_end_date,
          dg.drg_medical_surgical_ind,
          dg.chois_product_line_code,
          dg.chois_product_line_desc
   FROM {{ params.param_auth_base_views_dataset_name }}.diagnosis_related_group AS dg
   WHERE upper(rtrim(dg.reimbursement_group_name)) = 'M' ) AS drg_pa ON upper(rtrim(drg_pa.reimbursement_group_code)) = upper(rtrim(fp.drg_hcfa_icd10_code))
AND fp.discharge_date BETWEEN drg_pa.reimbursement_group_start_date AND drg_pa.reimbursement_group_end_date
LEFT OUTER JOIN
  (SELECT a.patient_dw_id,
          a.coid,
          a.company_code,
          a.drg_code AS apr_drg_code,
          a.drg_grouper_name AS apr_drg_grouper_name,
          si.severity_of_illness_desc AS apr_severity_of_illness_desc,
          rm.risk_of_mortality_desc AS apr_risk_of_mortality_desc
   FROM {{ params.param_auth_base_views_dataset_name }}.admission_drg AS a
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_apr_severity_of_illness AS si ON upper(trim(si.apr_severity_of_illness)) = upper(trim(a.apr_severity_of_illness))
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_apr_risk_of_mortality AS rm ON upper(trim(rm.apr_risk_of_mortality)) = upper(trim(a.apr_risk_of_mortality))
   WHERE upper(rtrim(a.company_code)) = 'H'
     AND upper(rtrim(a.drg_usage_code)) = 'A'
     AND CAST({{ params.param_bqutil_fns_dataset_name }}.cw_td_normalize_number(a.icd_version_ind) AS FLOAT64) = 0 QUALIFY row_number() OVER (PARTITION BY a.patient_dw_id
                                                                                                                                         ORDER BY a.pa_last_update_date DESC) = 1 ) AS apr ON apr.patient_dw_id = ccap.patient_dw_id
AND upper(rtrim(apr.coid)) = upper(rtrim(ccap.coid))
AND upper(rtrim(apr.company_code)) = upper(rtrim(ccap.company_code))
LEFT OUTER JOIN -- APR
 -- ICD 10

  (SELECT patient_condition_code.patient_dw_id,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) IN('XF', 'XG') THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_xf_xg_ind,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) = 'NU' THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_nu_ind,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) = 'NE' THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_ne_ind,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) = 'NS' THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_ns_ind,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) = 'NP' THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_np_ind,
          max(CASE
                  WHEN upper(rtrim(patient_condition_code.condition_code)) = 'NO' THEN 'Y'
                  ELSE 'N'
              END) AS cond_code_no_ind
   FROM {{ params.param_auth_base_views_dataset_name }}.patient_condition_code
   WHERE upper(rtrim(patient_condition_code.condition_code)) IN('XF',
                                                                'XG',
                                                                'NU',
                                                                'NE',
                                                                'NS',
                                                                'NP',
                                                                'NO')
   GROUP BY 1 QUALIFY row_number() OVER (PARTITION BY patient_condition_code.patient_dw_id
                                         ORDER BY patient_condition_code.patient_dw_id) = 1) AS condcode ON condcode.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN {{ params.param_parallon_pbs_views_dataset_name }}.registration_iplan_pf AS ri ON ri.patient_dw_id = ccap.patient_dw_id
AND ri.payor_dw_id = ccap.payor_dw_id
AND ri.iplan_insurance_order_num = ccap.iplan_order_num
AND upper(rtrim(ri.coid)) = upper(rtrim(ccap.coid))
AND upper(rtrim(ri.company_code)) = upper(rtrim(ccap.company_code))
AND ri.iplan_id = ccap.iplan_id
AND ri.eff_to_date = DATE '9999-12-31'
LEFT OUTER JOIN
  (SELECT -- -URS
 fp_0.company_code,
 fp_0.patient_dw_id,
 u.acct_with_denial_sw,
 u.all_day_authr_sw,
 CASE 1
     WHEN u.concurrent_overturned_peer_to_peer_sw THEN 'Y - Overturned'
     WHEN u.post_dchg_overturned_peer_to_peer_sw THEN 'Y - Overturned'
     WHEN u.concurrent_upheld_peer_to_peer_sw THEN 'Y - Upheld'
     WHEN u.post_dchg_upheld_peer_to_peer_sw THEN 'Y - Upheld'
     ELSE 'N - Not Performed'
 END AS ptp_performed_ind,
 u.acct_denial_rcvd_concurrent_sw,
 u.acct_denial_rcvd_post_disch_sw,
 u.acct_with_denial_date_not_doc_sw
   FROM {{ params.param_auth_base_views_dataset_name }}.ur_metric AS u
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp_0 ON fp_0.patient_dw_id = u.patient_dw_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(rtrim(ff.coid)) = upper(rtrim(fp_0.coid))
   AND upper(rtrim(ff.company_code)) = upper(rtrim(fp_0.company_code))
   WHERE upper(rtrim(fp_0.company_code)) = 'H'
     AND upper(rtrim(ff.coid_status_code)) = 'F'
     AND upper(rtrim(u.well_baby_ind)) = 'N'
     AND upper(fp_0.admission_patient_type_code) LIKE '%I%'
     AND (u.acct_with_denial_sw = 1
          OR u.concurrent_overturned_peer_to_peer_sw = 1
          OR u.post_dchg_overturned_peer_to_peer_sw = 1
          OR u.concurrent_upheld_peer_to_peer_sw = 1
          OR u.post_dchg_upheld_peer_to_peer_sw = 1
          OR u.all_day_authr_sw = 1
          OR u.acct_denial_rcvd_concurrent_sw = 1
          OR u.acct_denial_rcvd_post_disch_sw = 1
          OR u.acct_with_denial_date_not_doc_sw = 1) ) AS urs ON upper(rtrim(urs.company_code)) = upper(rtrim(ccap.company_code))
AND urs.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN -- Only bring back info that is relevant to calculations
 -- DNA Clinical Rationale

  (SELECT clm.patient_dw_id,
          clinrat.clin_rat_1,
          clinrat.clin_rat_2,
          clinrat.clin_rat_3,
          clinrat.clin_rat_4,
          clinrat.clin_rat_5,
          clinrat.clin_rat_6,
          clin6_comm.clin_rat_6_comm,
          clm.inpatient_only_procedure_waterpark_sw,
          clm.inpatient_only_procedure_user_sw
   FROM {{ params.param_auth_base_views_dataset_name }}.cm_dna_claim AS clm
   LEFT OUTER JOIN -- -clinical rationale fields pulling indicators to see what existed on the last claim

     (SELECT junc_cm_dna_claim_clinical_rationale.claim_id,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 1 THEN 1
                     ELSE 0
                 END) AS clin_rat_1,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 2 THEN 1
                     ELSE 0
                 END) AS clin_rat_2,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 3 THEN 1
                     ELSE 0
                 END) AS clin_rat_3,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 4 THEN 1
                     ELSE 0
                 END) AS clin_rat_4,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 5 THEN 1
                     ELSE 0
                 END) AS clin_rat_5,
             max(CASE
                     WHEN junc_cm_dna_claim_clinical_rationale.clinical_rationale_id = 6 THEN 1
                     ELSE 0
                 END) AS clin_rat_6
      FROM {{ params.param_auth_base_views_dataset_name }}.junc_cm_dna_claim_clinical_rationale
      WHERE junc_cm_dna_claim_clinical_rationale.eff_to_date_time = DATETIME '9999-12-31 00:00:00'
      GROUP BY 1) AS clinrat ON clinrat.claim_id = clm.claim_id
   LEFT OUTER JOIN
     (SELECT cr.claim_id,
             nt.note_text AS clin_rat_6_comm
      FROM {{ params.param_auth_base_views_dataset_name }}.junc_cm_dna_claim_clinical_rationale AS cr
      LEFT OUTER JOIN
        (SELECT nt_0.claim_id,
                nt_0.user_id,
                nt_0.source_created_date_time_utc,
                nt_0.note_text
         FROM {{ params.param_auth_base_views_dataset_name }}.cm_dna_note AS nt_0
         WHERE nt_0.note_type_id = 1
           AND upper(nt_0.note_text) LIKE '%RATIONALE:%'
           AND nt_0.eff_to_date_time = DATETIME '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY nt_0.claim_id,
                                                                                                              nt_0.user_id
                                                                                                 ORDER BY nt_0.source_created_date_time_utc DESC) = 1 ) AS nt ON nt.claim_id = cr.claim_id
      AND DATE(cr.source_created_date_time_utc) = DATE(nt.source_created_date_time_utc)
      AND nt.user_id = cr.user_id
      WHERE cr.clinical_rationale_id = 6
        AND cr.eff_to_date_time = DATETIME '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY cr.claim_id
                                                                                            ORDER BY cr.source_created_date_time_utc DESC) = 1 ) AS clin6_comm ON clin6_comm.claim_id = clm.claim_id
   WHERE clm.eff_to_date_time = DATETIME '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY clm.patient_dw_id
                                                                                          ORDER BY clm.claim_status_date_time_utc DESC, clm.claim_id DESC) = 1 ) AS dnaclm ON fp.patient_dw_id = dnaclm.patient_dw_id
LEFT OUTER JOIN -- -users can sometimes save the same note multiple times. Pull the last in this case
 -- -clin rationale can sometimes get placed on the same claim multiple times. Pull last in this case

  (SELECT p.patient_dw_id,
          p.coid,
          p.unit_num,
          p.pat_acct_num,
          e.midas_acct_num,
          p.payor_dw_id_ins1,
          p.iplan_id_ins1,
          CASE
              WHEN dend.midas_encounter_id IS NOT NULL
                   AND dod.date_of_denial IS NULL THEN 'Denial in Midas (No DNL Date)'
              WHEN dend.midas_encounter_id IS NOT NULL
                   AND dod.date_of_denial > p.discharge_date THEN 'Denial in Midas (Post Disch)'
              WHEN dend.midas_encounter_id IS NOT NULL
                   AND dod.date_of_denial <= p.discharge_date THEN 'Concurrent Denial (Pre Disch)'
              WHEN dend.midas_encounter_id IS NULL
                   AND dod.date_of_denial IS NULL THEN 'Not in Midas'
              ELSE 'Other'
          END AS denial_in_midas_status, -- DEN Documented
 dod.date_of_denial AS midas_date_of_denial,
 CASE
     WHEN fdf.patient_dw_id IS NOT NULL THEN 'Y'
     ELSE 'N'
 END AS all_days_approved_ind,
 CASE
     WHEN ptpp.midas_encounter_id IS NOT NULL
          AND ptpp.hcm_appeal_status_id IN(-- PTP Exists based on the 3 identified ID's
 33) THEN 'Y - Overturned' 
     WHEN ptpp.midas_encounter_id IS NOT NULL 
          AND ptpp.hcm_appeal_status_id IN(-- PTP OTD based on last appeal status for the 3 ID's
 -- PTP Exists based on the 3 identified ID's
 34, 
 38) THEN 'Y - Upheld' 
     ELSE 'N - Not Performed' 
 END AS ptp_performed, -- PTP OTD based on last appeal status for the 3 ID's
 xc.cm_last_xf_code_applied_date,
 xc.cm_xf_ind,
 xc.cm_last_xg_code_applied_date,
 xc.cm_xg_ind,
 appl.date_of_appeal AS last_appeal_date,
 appl.hcm_appeal_status_name AS last_appeal_status,
 appl.last_appeal_employee_id,
 appl.last_appeal_employee_name,
 rc.hcm_status_cause_name AS status_cause_name,
 miq4.hcm_disposition_desc AS last_conc_review_disp,
 epa.midas_principal_payer_auth_num,
 CASE
     WHEN upper(epa.midas_principal_payer_auth_num) LIKE '%1%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%2%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%3%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%4%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%5%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%6%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%7%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%8%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%9%'
          OR upper(epa.midas_principal_payer_auth_num) LIKE '%0%' THEN CASE
                                                                           WHEN epa.midas_principal_payer_auth_num IS NULL THEN 'No Auth #'
                                                                           WHEN upper(trim(epa.midas_principal_payer_auth_num)) = '' THEN 'No Auth #'
                                                                           WHEN strpos(epa.midas_principal_payer_auth_num, '/') = 0 THEN 'No "/" in Auth #'
                                                                           WHEN upper(rtrim(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2))) = '/I' THEN 'Inpatient (/I)'
                                                                           WHEN upper(rtrim(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2))) = '/V' THEN 'Observation (/V)'
                                                                           WHEN upper(rtrim(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2))) = '/S' THEN 'IP Skilled Nursing or Swing Bed (/S)'
                                                                           WHEN upper(rtrim(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2))) = '/B' THEN 'IP Behavioral Health (/B)'
                                                                           WHEN upper(rtrim(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 4))) = '/CPT' THEN 'Outpatient (/CPT)'
                                                                           ELSE 'Other'
                                                                       END
     ELSE epa.midas_principal_payer_auth_num
 END AS midas_principal_payer_auth_type,
 miq.iq_review_criteria_met_desc AS cm_last_iq_review_criteria_met_desc,
 miq.interqual_review_version_desc AS cm_last_iq_review_version_desc,
 miq.interqual_review_subset_desc AS cm_last_iq_review_subset_desc
   FROM {{ params.param_auth_base_views_dataset_name }}.fact_patient AS p
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS e ON e.patient_dw_id = p.patient_dw_id
   AND upper(rtrim(e.company_code)) = upper(rtrim(p.company_code))
   AND upper(rtrim(e.active_dw_ind)) = 'Y'
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_encounter_payer AS epa ON epa.midas_encounter_id = e.midas_encounter_id
   AND upper(rtrim(epa.active_dw_ind)) = 'Y'
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_employee AS ep ON ep.empl_id = e.iq_adm_initial_review_empl_id
   AND upper(rtrim(ep.active_dw_ind)) = 'Y'
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_cm_location AS lo ON lo.location_id = e.iq_adm_rev_location_id
   AND upper(rtrim(lo.active_dw_ind)) = 'Y'
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(rtrim(ff.coid)) = upper(rtrim(p.coid))
   AND upper(rtrim(ff.company_code)) = upper(rtrim(p.company_code))
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_facility AS cmf ON upper(rtrim(cmf.coid)) = upper(rtrim(e.coid))
   AND upper(rtrim(cmf.midas_facility_code)) = upper(rtrim(e.midas_facility_code))
   AND upper(rtrim(cmf.company_code)) = upper(rtrim(e.company_code))
   AND upper(rtrim(cmf.active_dw_ind)) = 'Y'
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_ed_event_date_time AS cl ON cl.patient_dw_id = e.patient_dw_id
   AND upper(rtrim(cl.company_code)) = upper(rtrim(e.company_code))
   LEFT OUTER JOIN -- Get Greet Date, Use in 2 Midnight Calc [Logic Per CM]
 -- Get The Initial Patient Type and Admit From Date, Use in 2 Midnight Calc [Logic Per CM]

     (SELECT apt.patient_dw_id,
             apt.eff_from_date,
             apt.admission_patient_type_code,
             row_number() OVER (PARTITION BY apt.patient_dw_id
                                ORDER BY apt.patient_dw_id,
                                         apt.eff_from_date) AS rec_num
      FROM {{ params.param_auth_base_views_dataset_name }}.admission_patient_type AS apt
      WHERE upper(rtrim(apt.admission_patient_type_code)) NOT IN('EP',
                                                                 'OP',
                                                                 'SP',
                                                                 'IP',
                                                                 'ER',
                                                                 'OR',
                                                                 'SR',
                                                                 'ERV',
                                                                 'ORV',
                                                                 'SRV',
                                                                 'N') ) AS ipt ON ipt.patient_dw_id = p.patient_dw_id
   AND ipt.rec_num = 1
   LEFT OUTER JOIN -- Bring Back the First Date of Denial

     (SELECT ed.midas_encounter_id,
             ia.date_of_denial
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS ed
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day AS ds ON ds.midas_encounter_id = ed.midas_encounter_id
      AND upper(rtrim(ds.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day_info AS ia ON ia.hcm_avoid_denied_day_id = ds.hcm_avoid_denied_day_id
      AND upper(rtrim(ia.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(ed.active_dw_ind)) = 'Y'
        AND ia.date_of_denial IS NOT NULL QUALIFY row_number() OVER (PARTITION BY ed.midas_encounter_id
                                                                     ORDER BY ia.date_of_denial) = 1 ) AS dod ON dod.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Population where a DOD exists
 -- Get Count of Concurrent Reviews and number of IQ ran

     (SELECT DISTINCT cr.midas_encounter_id,
                      count(rc_0.conc_rev_conc_rev_id) AS conc_review_cnt,
                      count(rc_0.interqual_review_code) AS conc_iq_cnt
      FROM {{ params.param_auth_base_views_dataset_name }}.hcm_concurrent_review AS cr
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_conc_rev_conc_rev AS rc_0 ON rc_0.concurrent_review_id = cr.concurrent_review_id
      AND upper(rtrim(rc_0.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(cr.active_dw_ind)) = 'Y'
      GROUP BY 1) AS conccnt ON conccnt.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Get The Last instance of a PTP Performed ID [Defined on 12/07/2018 - Alaga, David, Marcus & Daniel]

     (SELECT ec1.midas_encounter_id,
             app1.hcm_avoid_denied_apel_id,
             app1.date_of_appeal,
             app1.hcm_appeal_status_id,
             apps1.hcm_appeal_status_name
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS ec1
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
      AND upper(rtrim(dy1.company_code)) = upper(rtrim(ec1.company_code))
      AND upper(rtrim(dy1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
      AND upper(rtrim(dyi1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_appeal AS app1 ON app1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
      AND app1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
      AND upper(rtrim(app1.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_hcm_appeal_status AS apps1 ON apps1.hcm_appeal_status_id = app1.hcm_appeal_status_id
      AND upper(rtrim(apps1.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(ec1.active_dw_ind)) = 'Y'
        AND app1.hcm_appeal_status_id IN(33,
                                         34,
                                         38) QUALIFY row_number() OVER (PARTITION BY ec1.midas_encounter_id
                                                                        ORDER BY app1.hcm_avoid_denied_apel_id DESC) = 1 ) AS ptpp ON ptpp.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- P2P Complete - overturned, P2P Complete - payor upheld, P2P Complete - Partially Overturned
 -- Root Cause

     (SELECT ec1.midas_encounter_id,
             att1.hcm_status_cause_id,
             atts1.hcm_status_cause_name
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS ec1
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
      AND upper(rtrim(dy1.company_code)) = upper(rtrim(ec1.company_code))
      AND upper(rtrim(dy1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
      AND upper(rtrim(dyi1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_attribution AS att1 ON att1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
      AND att1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
      AND upper(rtrim(att1.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_hcm_status_cause AS atts1 ON atts1.hcm_status_cause_id = att1.hcm_status_cause_id
      AND upper(rtrim(atts1.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(ec1.active_dw_ind)) = 'Y'
        AND upper(atts1.hcm_status_cause_name) LIKE 'DEN - %'
        AND att1.hcm_status_cause_id IS NOT NULL QUALIFY row_number() OVER (PARTITION BY ec1.midas_encounter_id
                                                                            ORDER BY att1.hcm_avoid_denied_attr_id DESC) = 1 ) AS rc ON rc.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Get The Last Appeal Status Information on the encounter

     (SELECT ec1.midas_encounter_id,
             app1.hcm_avoid_denied_apel_id,
             app1.date_of_appeal,
             app1.hcm_appeal_status_id,
             apps1.hcm_appeal_status_name,
             coalesce(e_0.empl_num, e1.i1_empl_num) AS last_appeal_employee_id,
             coalesce(e_0.empl_name, e1.i2_empl_name) AS last_appeal_employee_name
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS ec1
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
      AND upper(rtrim(dy1.company_code)) = upper(rtrim(ec1.company_code))
      AND upper(rtrim(dy1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
      AND upper(rtrim(dyi1.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_appeal AS app1 ON app1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
      AND app1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
      AND upper(rtrim(app1.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_hcm_appeal_status AS apps1 ON apps1.hcm_appeal_status_id = app1.hcm_appeal_status_id
      AND upper(rtrim(apps1.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_employee AS e_0 ON e_0.empl_id = app1.system_user_id
      AND upper(rtrim(e_0.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN
        (SELECT cm_employee_inner.empl_id AS i0_empl_id,
                cm_employee_inner.empl_num AS i1_empl_num,
                cm_employee_inner.empl_name AS i2_empl_name,
                cm_employee_inner.active_dw_ind AS i3_active_dw_ind,
                cm_employee_inner.dw_last_update_date_time AS i4_dw_last_update_date_time
         FROM {{ params.param_auth_base_views_dataset_name }}.cm_employee AS cm_employee_inner
         WHERE cm_employee_inner.dw_last_update_date_time =
             (SELECT max(emax.dw_last_update_date_time)
              FROM {{ params.param_auth_base_views_dataset_name }}.cm_employee AS emax
              WHERE upper(rtrim(emax.active_dw_ind)) = 'N'
                AND emax.empl_id = cm_employee_inner.empl_id ) ) AS e1 ON e1.i0_empl_id = app1.system_user_id
      AND upper(rtrim(e1.i3_active_dw_ind)) = 'N'
      WHERE upper(rtrim(ec1.active_dw_ind)) = 'Y' QUALIFY row_number() OVER (PARTITION BY ec1.midas_encounter_id
                                                                             ORDER BY app1.hcm_avoid_denied_apel_id DESC) = 1 ) AS appl ON appl.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Get the last occurance of IQ ran

     (SELECT crr.midas_encounter_id,
             crre.empl_name AS conc_reviewer,
             crre.empl_num AS conc_reviewer_id,
             rcc.interqual_review_code,
             iqm.iq_review_criteria_met_desc,
             pri.interqual_review_version_desc,
             siq.interqual_review_subset_desc,
             iq.primary_review_start_date_time
      FROM {{ params.param_auth_base_views_dataset_name }}.hcm_concurrent_review AS crr
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_conc_rev_conc_rev AS rcc ON rcc.concurrent_review_id = crr.concurrent_review_id
      AND upper(rtrim(rcc.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_employee AS crre ON crre.empl_id = rcc.case_reviewed_by_employee_id
      AND upper(rtrim(crre.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_interqual_review AS iq ON upper(rtrim(iq.interqual_review_code)) = upper(rtrim(rcc.interqual_review_code))
      AND upper(rtrim(iq.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_cm_iq_criteria_met AS iqm ON iqm.iq_review_criteria_met_code = iq.iq_review_criteria_met_code
      AND upper(rtrim(iqm.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_interqual_review_product AS pri ON upper(rtrim(pri.interqual_review_product_code)) = upper(rtrim(iq.interqual_review_product_code))
      AND upper(rtrim(pri.interqual_review_version_code)) = upper(rtrim(iq.interqual_review_version_code))
      AND upper(rtrim(pri.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_interqual_review_subset AS siq ON upper(rtrim(siq.interqual_review_product_code)) = upper(rtrim(iq.interqual_review_product_code))
      AND upper(rtrim(siq.interqual_review_subset_code)) = upper(rtrim(iq.interqual_review_subset_code))
      AND upper(rtrim(siq.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(crr.active_dw_ind)) = 'Y' QUALIFY row_number() OVER (PARTITION BY crr.midas_encounter_id
                                                                             ORDER BY iq.primary_review_start_date_time DESC, upper(iq.interqual_review_code) DESC, iq.eff_from_date DESC) = 1 ) AS miq ON miq.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Get the Most Recent record
 -- Get the last Concurrent Reviewer

     (SELECT crr4.midas_encounter_id,
             crre4.empl_name AS conc_reviewer,
             crre4.empl_num AS conc_reviewer_id,
             rcc4.review_date,
             hcmd.hcm_disposition_code,
             hcmd.hcm_disposition_desc,
             rcc4.conc_rev_conc_rev_id
      FROM {{ params.param_auth_base_views_dataset_name }}.hcm_concurrent_review AS crr4
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_conc_rev_conc_rev AS rcc4 ON rcc4.concurrent_review_id = crr4.concurrent_review_id
      AND upper(rtrim(rcc4.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.cm_employee AS crre4 ON crre4.empl_id = rcc4.case_reviewed_by_employee_id
      AND upper(rtrim(crre4.active_dw_ind)) = 'Y'
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_hcm_disposition AS hcmd ON hcmd.disposition_id = rcc4.disposition_id
      AND upper(rtrim(hcmd.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(crr4.active_dw_ind)) = 'Y' QUALIFY row_number() OVER (PARTITION BY crr4.midas_encounter_id
                                                                              ORDER BY rcc4.conc_rev_conc_rev_id DESC) = 1 ) AS miq4 ON miq4.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- Get the Most Recent record
 -- Get Full Doc Indicator

     (SELECT fdi.patient_dw_id
      FROM {{ params.param_auth_base_views_dataset_name }}.full_documentation AS fdi
      WHERE upper(rtrim(fdi.active_dw_ind)) = 'Y'
        AND upper(rtrim(fdi.revised_full_doc_ind)) = 'Y'
        AND upper(rtrim(fdi.avoidable_denied_days_ind)) = 'N' ) AS fdf ON fdf.patient_dw_id = p.patient_dw_id
   LEFT OUTER JOIN -- Active Record
 -- Has Full Docs
 -- Not in Avoid Days
 -- Get Flags for X Coded Accounts

     (SELECT x.midas_encounter_id,
             max(CASE
                     WHEN upper(rtrim(x.xu_billing_condition_code_name)) = 'XF' THEN x.eff_from_date
                     ELSE CAST(NULL AS DATE)
                 END) AS cm_last_xf_code_applied_date,
             max(CASE
                     WHEN upper(rtrim(x.xu_billing_condition_code_name)) = 'XF' THEN 'Y'
                     ELSE 'N'
                 END) AS cm_xf_ind,
             max(CASE
                     WHEN upper(rtrim(x.xu_billing_condition_code_name)) = 'XG' THEN x.eff_from_date
                     ELSE CAST(NULL AS DATE)
                 END) AS cm_last_xg_code_applied_date,
             max(CASE
                     WHEN upper(rtrim(x.xu_billing_condition_code_name)) = 'XG' THEN 'Y'
                     ELSE 'N'
                 END) AS cm_xg_ind
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_billing_condition_code AS x
      WHERE upper(rtrim(x.active_dw_ind)) = 'Y'
        AND upper(rtrim(x.xu_billing_condition_code_name)) IN('XF',
                                                              'XG')
      GROUP BY 1) AS xc ON xc.midas_encounter_id = e.midas_encounter_id
   LEFT OUTER JOIN -- , 'XN', 'X3', 'XH', 'XX', 'X2', 'XS'
 -- Flag if a Denial is documented on the Avoidable days screen

     (SELECT DISTINCT ed5.midas_encounter_id
      FROM {{ params.param_auth_base_views_dataset_name }}.cm_encounter AS ed5
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day AS ds5 ON ds5.midas_encounter_id = ed5.midas_encounter_id
      AND upper(rtrim(ds5.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_day_info AS ia5 ON ia5.hcm_avoid_denied_day_id = ds5.hcm_avoid_denied_day_id
      AND upper(rtrim(ia5.active_dw_ind)) = 'Y'
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_hcm_days_type AS ty ON ty.hcm_days_type_id = ia5.type_of_day_id
      AND upper(rtrim(ty.active_dw_ind)) = 'Y'
      WHERE upper(rtrim(ed5.active_dw_ind)) = 'Y'
        AND upper(ty.hcm_days_type_name) LIKE '%DEN %' ) AS dend ON dend.midas_encounter_id = e.midas_encounter_id
   WHERE upper(rtrim(ff.coid_status_code)) = 'F'
     AND trim(cmf.midas_facility_code) IS NOT NULL QUALIFY row_number() OVER (PARTITION BY p.patient_dw_id
                                                                              ORDER BY e.dw_last_update_date_time DESC) = 1 ) AS cmd ON cmd.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN -- P.Company_Code = 'H' --HCA Only
 -- Hospitals I believe
 -- Get the last Encounter if there are ever multiple

  (SELECT fp_0.patient_dw_id,
          trim(pduu.determination_reason_desc) AS pdu_determination_reason_desc
   FROM {{ params.param_auth_base_views_dataset_name }}.prebill_denial_detail AS pduu
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp_0 ON upper(rtrim(fp_0.company_code)) = upper(rtrim(pduu.company_code))
   AND upper(rtrim(fp_0.coid)) = upper(rtrim(pduu.coid))
   AND fp_0.pat_acct_num = pduu.pat_acct_num
   WHERE upper(rtrim(pduu.initial_auth_status_code)) <> 'EXCEPTION'
     AND (upper(trim(pduu.pilot_acct_ind)) = 'N'
          OR trim(pduu.pilot_acct_ind) IS NULL) QUALIFY row_number() OVER (PARTITION BY upper(pduu.coid),
                                                                                        pduu.pat_acct_num
                                                                           ORDER BY pduu.rptg_date DESC) = 1 ) AS pdu ON pdu.patient_dw_id = ccap.patient_dw_id
LEFT OUTER JOIN -- AND Rptg_Date=(SELECT MAX(Rptg_Date) FROM  EDWPBS_Views.Prebill_Denial_Detail)
 -- Bring back the most recent status for the account / coid

  (SELECT fn.patient_dw_id,
          fn.reporting_date,
          fn.pat_acct_num,
          fn.reporting_payor_dw_id,
          fn.reporting_iplan_id,
          fn.reporting_iplan_seq_num, -- FN.IB_MedRec_Doc_Req_Desc AS Doc_Request_Type ,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_medrec_request_date AS STRING))), '1900-01-01')) AS DATE) AS min_doc_req_medrec_request_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_medrec_request_date AS STRING))), '1900-01-01')) AS DATE) AS max_doc_req_medrec_request_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_medrec_sent_date AS STRING))), '1900-01-01')) AS DATE) AS min_doc_req_medrec_sent_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_medrec_sent_date AS STRING))), '1900-01-01')) AS DATE) AS max_doc_req_medrec_sent_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_ib_request_date AS STRING))), '1900-01-01')) AS DATE) AS min_doc_req_ib_request_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_ib_request_date AS STRING))), '1900-01-01')) AS DATE) AS max_doc_req_ib_request_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_ib_sent_date AS STRING))), '1900-01-01')) AS DATE) AS min_doc_req_ib_sent_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_ib_sent_date AS STRING))), '1900-01-01')) AS DATE) AS max_doc_req_ib_sent_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_request_date AS STRING))), '1900-01-01')) AS DATE) AS first_doc_request_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_request_date AS STRING))), '1900-01-01')) AS DATE) AS last_doc_request_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_sent_date AS STRING))), '1900-01-01')) AS DATE) AS first_doc_sent_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_sent_date AS STRING))), '1900-01-01')) AS DATE) AS last_doc_sent_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_received_date AS STRING))), '1900-01-01')) AS DATE) AS first_doc_received_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_received_date AS STRING))), '1900-01-01')) AS DATE) AS last_doc_received_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_approve_date AS STRING))), '1900-01-01')) AS DATE) AS first_doc_approved_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_approve_date AS STRING))), '1900-01-01')) AS DATE) AS last_doc_approved_date,
 CAST(trim(coalesce(min(trim(CAST(fn.doc_req_denied_date AS STRING))), '1900-01-01')) AS DATE) AS first_doc_denied_date,
 CAST(trim(coalesce(max(trim(CAST(fn.doc_req_denied_date AS STRING))), '1900-01-01')) AS DATE) AS last_doc_denied_date
   FROM /***********************************************************************************************************
          								Sub-Select Statement - Get All Artiva Document Request Level Inventory
          			************************************************************************************************************/
     (SELECT d.patient_dw_id,
             d.reporting_date,
             d.artiva_instance_code,
             d.coid,
             d.pat_acct_num,
             coalesce(p.total_billed_charges, CAST(0 AS NUMERIC)) AS total_billed_charges_amt,
             ROUND(CASE
                       WHEN d.iplan_id = p.iplan_id_ins1 THEN p.payor_dw_id_ins1
                       WHEN d.iplan_id = p.iplan_id_ins2 THEN p.payor_dw_id_ins2
                       WHEN d.iplan_id = p.iplan_id_ins3 THEN p.payor_dw_id_ins3
                       WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i1.major_payor_group_id)) THEN p.payor_dw_id_ins1
                       WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i2.major_payor_group_id)) THEN p.payor_dw_id_ins2
                       WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i3.major_payor_group_id)) THEN p.payor_dw_id_ins3
                       ELSE CAST(0 AS NUMERIC)
                   END, 0, 'ROUND_HALF_EVEN') AS reporting_payor_dw_id,
             CASE
                 WHEN d.iplan_id = p.iplan_id_ins1 THEN d.iplan_id
                 WHEN d.iplan_id = p.iplan_id_ins2 THEN d.iplan_id
                 WHEN d.iplan_id = p.iplan_id_ins3 THEN d.iplan_id
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i1.major_payor_group_id)) THEN p.iplan_id_ins1
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i2.major_payor_group_id)) THEN p.iplan_id_ins2
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i3.major_payor_group_id)) THEN p.iplan_id_ins3
                 ELSE 0
             END AS reporting_iplan_id,
             CASE
                 WHEN d.iplan_id = p.iplan_id_ins1 THEN 1
                 WHEN d.iplan_id = p.iplan_id_ins2 THEN 2
                 WHEN d.iplan_id = p.iplan_id_ins3 THEN 3
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i1.major_payor_group_id)) THEN 1
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i2.major_payor_group_id)) THEN 2
                 WHEN upper(rtrim(i.major_payor_group_id)) = upper(rtrim(i3.major_payor_group_id)) THEN 3
                 ELSE 0
             END AS reporting_iplan_seq_num,
             d.iplan_id AS doc_req_iplan_id,
             i.plan_desc AS doc_req_iplan_desc,
             d.status_code AS doc_req_status,
             d.create_date AS doc_req_submit_date,
             d.sent_date AS doc_req_sent_date,
             d.received_date AS doc_req_received_date,
             d.requested_date AS doc_req_request_date,
             d.approved_date AS doc_req_approve_date,
             d.denied_date AS doc_req_denied_date,
             d.med_rec_req_date AS doc_req_medrec_request_date,
             d.med_rec_sent_date AS doc_req_medrec_sent_date,
             d.itm_bill_req_date AS doc_req_ib_request_date,
             d.itm_bill_sent_date AS doc_req_ib_sent_date,
             d.first_letter_sent_date AS doc_req_first_letter_sent_date,
             d.second_letter_sent_date AS doc_req_second_letter_sent_date,
             d.third_letter_sent_date AS doc_req_third_letter_sent_date,
             CASE
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_ind), 'N'))) = 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_ind), 'N'))) = 'Y' THEN 'IB & MR Request'
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_ind), 'N'))) = 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_ind), 'N'))) <> 'Y' THEN 'Medical Record Request'
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_ind), 'N'))) <> 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_ind), 'N'))) = 'Y' THEN 'Itemized Bill Request'
                 ELSE 'All Other'
             END AS ib_medrec_doc_req_desc,
             CASE
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_aprv_ind), 'N'))) = 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_aprv_ind), 'N'))) = 'Y' THEN 'IB & MR Approved'
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_aprv_ind), 'N'))) = 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_aprv_ind), 'N'))) <> 'Y' THEN 'Medical Record Approved'
                 WHEN upper(rtrim(coalesce(trim(d.cplt_med_rec_aprv_ind), 'N'))) <> 'Y'
                      AND upper(rtrim(coalesce(trim(d.itm_bill_aprv_ind), 'N'))) = 'Y' THEN 'Itemized Bill Approved'
                 ELSE 'All Other'
             END AS ib_medrec_doc_req_apr_desc
      FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.collection_doc_req_dtl AS d
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd ON upper(rtrim(fd.coid)) = upper(rtrim(d.coid))
      AND upper(rtrim(fd.company_code)) = upper(rtrim(d.company_code))
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS i ON upper(rtrim(i.coid)) = upper(rtrim(d.coid))
      AND i.iplan_id = d.iplan_id
      AND upper(rtrim(i.company_code)) = upper(rtrim(d.company_code))
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS p ON p.patient_dw_id = d.patient_dw_id
      AND upper(rtrim(p.company_code)) = upper(rtrim(d.company_code))
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS i1 ON i1.payor_dw_id = p.payor_dw_id_ins1
      AND upper(rtrim(i1.coid)) = upper(rtrim(p.coid))
      AND upper(rtrim(i1.company_code)) = upper(rtrim(p.company_code))
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS i2 ON i2.payor_dw_id = p.payor_dw_id_ins2
      AND upper(rtrim(i2.coid)) = upper(rtrim(p.coid))
      AND upper(rtrim(i2.company_code)) = upper(rtrim(p.company_code))
      LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS i3 ON i3.payor_dw_id = p.payor_dw_id_ins3
      AND upper(rtrim(i3.coid)) = upper(rtrim(p.coid))
      AND upper(rtrim(i3.company_code)) = upper(rtrim(p.company_code))
      WHERE upper(rtrim(d.company_code)) = 'H'
        AND upper(rtrim(fd.summary_7_member_ind)) = 'Y'
        AND upper(rtrim(coalesce(fd.osg_pas_ind, 'N'))) <> 'Y'
        AND upper(rtrim(coalesce(fd.summary_asd_member_ind, 'N'))) <> 'Y'
        AND upper(rtrim(coalesce(fd.summary_imaging_member_ind, 'N'))) <> 'Y'
        AND upper(rtrim(fd.pas_coid)) IN('08591',
                                         '08648',
                                         '08942',
                                         '08945',
                                         '08947',
                                         '08948',
                                         '08949',
                                         '08950')
        AND CAST(i.iplan_financial_class_code AS INT64) IN(5,
                                                           7,
                                                           8,
                                                           9,
                                                           11,
                                                           12,
                                                           13,
                                                           14)
        AND upper(rtrim(coalesce(i.payer_type_code, ''))) NOT IN('MV',
                                                                 'VA')
        AND rtrim(coalesce(i.major_payor_group_id, format('%4d', 0))) NOT IN(-- Motor Vehicle or Veteran Admin
 '930')
        AND upper(rtrim(coalesce(d.status_code, ''))) <> 'CANC'
        AND d.create_date >= DATE '2020-01-01'
        AND d.reporting_date =
          (SELECT -- Veteran Admin
 -- Removed Canceled Request's
 max(dm.reporting_date) AS maxreporting_date 
           FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.collection_doc_req_dtl AS dm) ) AS fn 
   GROUP BY 1, 
            2, 
            3, 
            4, 
            5, 
            6) AS prepay ON prepay.patient_dw_id = ccap.patient_dw_id 
AND prepay.reporting_payor_dw_id = ccap.payor_dw_id 
AND prepay.reporting_iplan_seq_num = ccap.iplan_order_num 
LEFT OUTER JOIN 
  (SELECT pd.patient_dw_id 
   FROM {{ params.param_auth_base_views_dataset_name }}.patient_diagnosis AS pd 
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp_0 ON upper(rtrim(pd.company_code)) = upper(rtrim(fp_0.company_code)) 
   AND fp_0.patient_dw_id = pd.patient_dw_id 
   WHERE upper(trim(pd.diag_code)) = 'B9729' 
     AND fp_0.discharge_date BETWEEN DATE '2020-01-27' AND DATE '2020-03-31' 
     OR upper(trim(pd.diag_code)) = 'U071' 
     AND fp_0.discharge_date >= DATE '2020-04-01' QUALIFY row_number() OVER (PARTITION BY upper(pd.coid), 
                                                                                          pd.pat_acct_num
                                                                             ORDER BY pd.pa_last_update_date DESC) = 1 ) AS cov ON cov.patient_dw_id = ccap.patient_dw_id 
LEFT OUTER JOIN -- Refunds to Payer
 
  (SELECT cri.patient_dw_id, 
          cri.refund_iplan_id, 
          cri.refund_amt, 
          cri.entered_date, 
          cri.refund_creation_user_id, 
          cri.last_update_date_time 
   FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS cri 
   WHERE cri.reporting_date = 
       (SELECT max(crz.reporting_date) 
        FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS crz) 
     AND cri.refund_type_sid IN(2, 
                                3) QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id, 
                                                                           cri.refund_iplan_id
                                                              ORDER BY cri.dw_last_update_date_time DESC) = 1 ) AS cr1py ON cr1py.patient_dw_id = ccap.patient_dw_id 
AND cr1py.refund_iplan_id = ccap.iplan_id 
LEFT OUTER JOIN -- Primary & Other Payors
 -- Refunds to Patient (Applied at account level)
 
  (SELECT cri.patient_dw_id, 
          cri.refund_amt, 
          cri.entered_date, 
          cri.refund_creation_user_id, 
          cri.last_update_date_time 
   FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS cri 
   WHERE cri.reporting_date = 
       (SELECT max(crz.reporting_date) 
        FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS crz) 
     AND cri.refund_type_sid = 4 QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id
                                                            ORDER BY cri.dw_last_update_date_time DESC) = 1 ) AS cr1pt ON cr1pt.patient_dw_id = ccap.patient_dw_id 
AND ccap.iplan_order_num = 1 
LEFT OUTER JOIN -- Patient Refunds
 -- Credit Status
 
  (SELECT cri.patient_dw_id, 
          CASE 
              WHEN coalesce(cri.refund_iplan_id, 0) = 0 THEN cri.iplan_id_ins1 
              ELSE cri.refund_iplan_id 
          END AS iplan_id, 
          cri.refund_type_sid, 
          eiscs.credit_status_alias, 
          cri.last_update_date_time 
   FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS cri 
   LEFT OUTER JOIN {{ params.param_parallon_pbs_base_views_dataset_name }}.eis_credit_status_dim AS eiscs ON cri.credit_status_sid = eiscs.credit_status_sid 
   WHERE cri.reporting_date = 
       (SELECT max(crz.reporting_date) 
        FROM {{ params.param_parallon_pbs_views_dataset_name }}.payment_compliance_credit_inventory AS crz) QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id, 
                                                                                                                                         iplan_id
                                                                                                                            ORDER BY cri.dw_last_update_date_time DESC) = 1 ) AS crb ON crb.patient_dw_id = ccap.patient_dw_id 
AND crb.iplan_id = ccap.iplan_id 
LEFT OUTER JOIN -- AND CRI.Credit_Status_Sid IS NOT NULL
 -- AND ZeroIfNull(CRI.Credit_Status_Sid) <> 1 -- No Credit Status
 
  (SELECT crt.patient_dw_id, 
          crt.discrepancy_source_desc, 
          crt.reimbursement_impact_desc, 
          crt.discrepancy_date_time, 
          crt.request_date_time, 
          crt.reprocess_reason_text, 
          crt.status_desc 
   FROM {{ params.param_parallon_pbs_base_views_dataset_name }}.claim_reprocessing_tool_detail AS crt QUALIFY row_number() OVER (PARTITION BY crt.patient_dw_id
                                                                                                                 ORDER BY crt.crt_log_id DESC) = 1) AS msccr ON msccr.patient_dw_id = ccap.patient_dw_id 
LEFT OUTER JOIN 
  (SELECT ccap_0.patient_dw_id, 
          ccap_0.iplan_id, 
          ccap_0.iplan_order_num, 
          CASE 
              WHEN ipbill.first_11x_bill_date = opbill.first_13x_bill_date 
                   AND ipbill.first_11x_bill_date IS NOT NULL THEN 'Y' 
              WHEN maxopbill.max_13x_billed_charges <= fp_0.total_billed_charges * NUMERIC '0.8' THEN 'Y' 
              ELSE 'N' 
          END AS split_bill_ind 
   FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS ccap_0 
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp_0 ON fp_0.patient_dw_id = ccap_0.patient_dw_id 
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_iplan AS fi_0 ON fi_0.payor_dw_id = ccap_0.payor_dw_id 
   LEFT OUTER JOIN 
     (SELECT rh_837_claim.patient_dw_id, 
             rh_837_claim.iplan_id, 
             rh_837_claim.iplan_insurance_order_num, 
             max(rh_837_claim.bill_date) AS last_11x_bill_date, 
             min(rh_837_claim.bill_date) AS first_11x_bill_date 
      FROM {{ params.param_auth_base_views_dataset_name }}.rh_837_claim 
      WHERE upper(rtrim(substr(rh_837_claim.bill_type_code, 1, 2))) = '11' 
        AND upper(trim(rh_837_claim.bill_type_code)) <> '118' 
        AND upper(rtrim(CASE 
                            WHEN upper(rtrim(rh_837_claim.pas_coid)) = '08910' 
                                 AND upper(rtrim(rh_837_claim.facility_prefix_code)) IN(-- Exclude cancel claims
 -- Exclude DSH/IME claims billed by MSC
 'D', 'H') THEN 'N' 
                            ELSE 'Y' 
                        END)) = 'Y' 
      GROUP BY 1, 
               2, 
               3) AS ipbill ON ipbill.patient_dw_id = ccap_0.patient_dw_id 
   AND ipbill.iplan_id = ccap_0.iplan_id 
   AND ipbill.iplan_insurance_order_num = ccap_0.iplan_order_num 
   LEFT OUTER JOIN 
     (SELECT c.patient_dw_id, 
             c.iplan_id, 
             c.iplan_insurance_order_num, 
             max(c.bill_date) AS last_13x_bill_date, 
             min(c.bill_date) AS first_13x_bill_date 
      FROM {{ params.param_auth_base_views_dataset_name }}.rh_837_claim AS c 
      WHERE upper(rtrim(substr(c.bill_type_code, 1, 2))) = '13' 
        AND upper(rtrim(c.bill_type_code)) <> '130' 
        AND upper(rtrim(CASE 
                            WHEN upper(rtrim(c.pas_coid)) = '08910' 
                                 AND upper(rtrim(c.facility_prefix_code)) IN(-- Exclude DSH/IME claims billed by MSC
 'D', 'H') THEN 'N' 
                            ELSE 'Y' 
                        END)) = 'Y' 
      GROUP BY 1, 
               2, 
               3) AS opbill ON opbill.patient_dw_id = ccap_0.patient_dw_id 
   AND opbill.iplan_id = ccap_0.iplan_id 
   AND opbill.iplan_insurance_order_num = ccap_0.iplan_order_num 
   LEFT OUTER JOIN -- Get bill type of most recent 13x on account
 
     (SELECT c.patient_dw_id, 
             c.iplan_id, 
             c.iplan_insurance_order_num, 
             c.bill_type_code, 
             c.total_charge_amt AS max_13x_billed_charges 
      FROM {{ params.param_auth_base_views_dataset_name }}.rh_837_claim AS c 
      INNER JOIN 
        (SELECT rh_837_claim.patient_dw_id, 
                rh_837_claim.iplan_id, 
                rh_837_claim.iplan_insurance_order_num, 
                max(rh_837_claim.bill_date) AS maxbill 
         FROM {{ params.param_auth_base_views_dataset_name }}.rh_837_claim 
         WHERE upper(rtrim(substr(rh_837_claim.bill_type_code, 1, 2))) = '13' 
           AND upper(rtrim(rh_837_claim.bill_type_code)) <> '130' 
           AND upper(rtrim(CASE 
                               WHEN upper(rtrim(rh_837_claim.pas_coid)) = '08910' 
                                    AND upper(rtrim(rh_837_claim.facility_prefix_code)) IN(-- Exclude DSH/IME claims billed by MSC
 'D', 'H') THEN 'N' 
                               ELSE 'Y' 
                           END)) = 'Y' 
         GROUP BY 1, 
                  2, 
                  3) AS maxbill ON c.patient_dw_id = maxbill.patient_dw_id 
      AND c.iplan_id = maxbill.iplan_id 
      AND c.iplan_insurance_order_num = maxbill.iplan_insurance_order_num 
      AND c.bill_date = maxbill.maxbill 
      WHERE upper(rtrim(substr(c.bill_type_code, 1, 2))) = '13' 
        AND upper(rtrim(c.bill_type_code)) <> '130' 
        AND upper(rtrim(CASE 
                            WHEN upper(rtrim(c.pas_coid)) = '08910' 
                                 AND upper(rtrim(c.facility_prefix_code)) IN(-- Exclude DSH/IME claims billed by MSC
 'D', 'H') THEN 'N' 
                            ELSE 'Y' 
                        END)) = 'Y' 
        AND c.bill_date = maxbill.maxbill QUALIFY row_number() OVER (PARTITION BY c.patient_dw_id, 
                                                                                  c.iplan_id, 
                                                                                  c.iplan_insurance_order_num
                                                                     ORDER BY c.patient_dw_id) = 1 ) AS maxopbill ON ccap_0.patient_dw_id = maxopbill.patient_dw_id 
   AND ccap_0.iplan_id = maxopbill.iplan_id 
   AND ccap_0.iplan_order_num = maxopbill.iplan_insurance_order_num 
   WHERE ccap_0.iplan_order_num IN(1, 
                                   2, 
                                   3) 
     AND ccap_0.dw_last_update_date_time = 
       (SELECT max(ccap1.dw_last_update_date_time) 
        FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS ccap1 
        WHERE ccap1.patient_dw_id = ccap_0.patient_dw_id 
          AND ccap1.payor_dw_id = ccap_0.payor_dw_id 
          AND ccap1.iplan_order_num = ccap_0.iplan_order_num ) ) AS splitbill ON splitbill.patient_dw_id = ccap.patient_dw_id 
AND splitbill.iplan_id = ccap.iplan_id 
AND splitbill.iplan_order_num = ccap.iplan_order_num 
LEFT OUTER JOIN 
  (SELECT aa.patient_dw_id, 
          aa.payor_dw_id, 
          aa.iplan_insurance_order_num, 
          aa.activity_create_date_time AS last_scripted_applied_date_time, 
          aa.create_login_userid, 
          CASE 
              WHEN scr_0.patient_dw_id IS NOT NULL THEN scra.metric_alias_name 
              ELSE CAST(NULL AS STRING) 
          END AS scripted_overpayment_desc 
   FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS aa 
   LEFT OUTER JOIN {{ params.param_parallon_pbs_base_views_dataset_name }}.calculated_payor_overpayment AS scr_0 ON scr_0.patient_dw_id = aa.patient_dw_id 
   AND scr_0.payor_dw_id = aa.payor_dw_id 
   AND scr_0.rptg_date = DATE(aa.activity_create_date_time) 
   LEFT OUTER JOIN {{ params.param_parallon_pbs_views_dataset_name }}.dim_metric AS scra ON scra.metric_sid = coalesce(scr_0.overpayment_metric_sid, 10) 
   AND upper(rtrim(scra.metric_name_parent)) = 'POTENTIAL_OVERPAYMENT_CATEGORY' 
   WHERE aa.activity_type_id = 2 
     AND upper(aa.activity_subject_text) LIKE '%TO%SCRIPTED%FROM%' QUALIFY row_number() OVER (PARTITION BY aa.patient_dw_id, 
                                                                                                           aa.payor_dw_id, 
                                                                                                           aa.iplan_insurance_order_num
                                                                                              ORDER BY last_scripted_applied_date_time DESC) = 1 ) AS scr ON scr.patient_dw_id = ccap.patient_dw_id 
AND scr.payor_dw_id = ccap.payor_dw_id 
AND scr.iplan_insurance_order_num = ccap.iplan_order_num 
LEFT OUTER JOIN -- Nulls are
 -- Reason Change
 
  (SELECT aa.patient_dw_id, 
          aa.payor_dw_id, 
          aa.iplan_insurance_order_num, 
          aa.activity_create_date_time AS last_letter_sent_date_time 
   FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS aa 
   WHERE upper(trim(aa.activity_subject_text)) = 'POTENTIAL OVERPAYMENT LETTER SHIPPED - OEH' 
     OR upper(trim(aa.activity_subject_text)) = 'AUTOMATED POTENTIAL OVERPAYMENT LETTER SENT' QUALIFY row_number() OVER (PARTITION BY aa.patient_dw_id, 
                                                                                                                                      aa.payor_dw_id, 
                                                                                                                                      aa.iplan_insurance_order_num
                                                                                                                         ORDER BY last_letter_sent_date_time DESC) = 1 ) AS ltr ON ltr.patient_dw_id = ccap.patient_dw_id 
AND ltr.payor_dw_id = ccap.payor_dw_id 
AND ltr.iplan_insurance_order_num = ccap.iplan_order_num 
CROSS JOIN UNNEST(ARRAY[ CASE 
                             WHEN gov_flag.patient_dw_id IS NOT NULL THEN 'Y' 
                             ELSE 'N' 
                         END ]) AS gov_sec_tert_iplan 
WHERE ccap.iplan_order_num IN(-- Include primary, secondary and tertiary payors
 1, 
 2, 
 3) 
  AND ccap.dw_last_update_date_time = 
    (SELECT -- Include active account payor records
 max(ccap1.dw_last_update_date_time) 
     FROM {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS ccap1 
     WHERE ccap1.patient_dw_id = ccap.patient_dw_id 
       AND ccap1.iplan_order_num = ccap.iplan_order_num ) 
  AND upper(rtrim(cca.archive_state_ind)) = 'N'