-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/v_edw_daily_denial_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_denial_inventory AS SELECT
    d.schema_id,
    d.mon_account_payer_id,
    d.pass_type,
    d.coid,
    d.account_no,
    d.ssc_id,
    d.rate_schedule_name,
    d.ssc_name,
    d.facility_name,
    d.unit_num,
    d.rate_schedule_eff_begin_date,
    d.rate_schedule_eff_end_date,
    d.patient_name,
    d.patient_dob,
    d.iplan_id,
    d.insurance_provider_name,
    d.payer_group_name,
    d.billing_name,
    d.billing_contact_person,
    d.authorization_code,
    d.payer_patient_id,
    d.payer_rank,
    d.pa_financial_class,
    d.payor_financial_class,
    d.accounting_period,
    d.major_payer_grp,
    d.reason_code,
    d.billing_status,
    d.pa_service_code,
    d.pa_account_status,
    d.cc_patient_type,
    d.pa_discharge_status,
    d.pa_patient_type,
    d.cancel_bill_ind,
    d.admit_source,
    d.admit_type,
    d.pa_drg,
    d.remit_drg_code,
    d.attending_physician_id,
    d.attending_physician_name,
    d.service_date_begin,
    d.discharge_date,
    d.comparison_method,
    d.project_name,
    d.work_queue_name,
    d.status_category_desc,
    d.status_desc,
    d.status_phase_desc,
    d.calc_date,
    d.total_charges,
    d.pa_actual_los,
    d.total_billed_charges,
    d.covered_charges,
    d.total_expected_payment,
    d.total_expected_adjustment,
    d.total_pt_responsibility_actual,
    d.total_variance_adjustment,
    d.total_payments,
    d.total_denial_amt,
    d.payor_due_amt,
    d.pa_total_account_bal,
    d.ar_amount,
    d.otd_amt_net,
    d.writeoff_amt_net,
    d.cash_adj_amt_net,
    d.otd_to_date_amount_mtd,
    d.writeoff_amt_mtd,
    d.cash_adj_amt_mtd,
    d.max_aplno,
    d.max_seqno,
    d.appeal_orig_amt,
    d.current_appealed_amt,
    d.current_appeal_balance,
    d.appeal_date_created,
    d.sequence_date_created,
    d.close_date,
    d.max_seq_deadline_date,
    d.sequence_creator,
    d.appeal_owner,
    d.appeal_modifier,
    d.disp_code,
    d.disp_desc,
    d.web_disp_code,
    d.web_disposition_type,
    d.root_code,
    d.root_cause_description,
    d.root_cause_dtl,
    CASE
      WHEN trim(d.external_appeal_code) IS NULL THEN mp.external_appeal_code
      ELSE d.external_appeal_code
    END AS external_appeal_code,
    d.apl_appeal_code,
    d.apl_appeal_desc,
    d.first_denial_date,
    d.denial_age,
    d.pa_denial_update_date,
    d.first_activity_create_date,
    d.last_activity_completion_date,
    d.last_activity_completion_age,
    d.last_user_activity_create_age,
    d.last_reason_change_date,
    d.last_status_change_date,
    d.last_project_change_date,
    d.last_owner_change_date,
    d.activity_due_date,
    d.activity_desc,
    d.activity_subject,
    d.activity_owner,
    d.appeal_sent_activity_ownr,
    d.appeal_initiation_date,
    d.appeal_sent_activity_date,
    d.appeal_sent_activity_age,
    d.last_status_change_age,
    d.activity_due_date_age,
    d.latest_seq_creation_date_age,
    d.appeal_deadline_days_remaining,
    d.appeal_sent_activity_subj,
    d.appeal_sent_activity_desc,
    d.extract_date,
    d.payer_category,
    d.source_system_code,
    d.seq_no,
    d.dw_last_update_date,
    d.row_count,
    d.expected_amt,
    d.new_appeal_flag,
    d.disposition_code_modified_date,
    d.disposition_code_modified_by,
    d.vendor_code,
    d.sub_unit_num,
    d.latest_iplan_change_date_pa,
    d.artiva_activity_due_date,
    appsent_excrpt_disp_flag AS appsent_excrpt_disp_flag,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 'Not Stratified'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 'Not Stratified'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 'Not Stratified'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 'Not Stratified'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 'Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Non-Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'At Risk'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'At Risk'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Compliant'
      ELSE 'Undefined'
    END AS status_kpi,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 'Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 'At Risk'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 'At Risk'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Non-Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 'Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 'Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 'At Risk'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 'At Risk'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 'Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 'Non-Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS followup_kpi,
    appealsent_kpi AS appealsent_kpi,
    deadline_kpi AS deadline_kpi,
    CASE
      WHEN upper(rtrim(appealsent_kpi)) = 'NON-COMPLIANT'
       OR upper(rtrim(deadline_kpi)) = 'NON-COMPLIANT' THEN 'Non-Compliant'
      WHEN (upper(rtrim(appealsent_kpi)) = 'AT RISK'
       OR upper(rtrim(deadline_kpi)) = 'AT RISK')
       AND upper(rtrim(appealsent_kpi)) <> 'NON-COMPLIANT'
       AND upper(rtrim(deadline_kpi)) <> 'NON-COMPLIANT' THEN 'At Risk'
      WHEN (upper(rtrim(appealsent_kpi)) = 'COMPLIANT'
       OR upper(rtrim(deadline_kpi)) = 'COMPLIANT')
       AND upper(appealsent_kpi) NOT IN(
        'NON-COMPLIANT', 'AT RISK'
      )
       AND upper(deadline_kpi) NOT IN(
        'NON-COMPLIANT', 'AT RISK'
      ) THEN 'Compliant'
      WHEN upper(rtrim(appealsent_kpi)) = 'UNDEFINED'
       AND upper(rtrim(deadline_kpi)) = 'UNDEFINED' THEN 'Undefined'
      ELSE 'Undefined'
    END AS appealsent_deadline_kpi,
    CASE
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age <= -2 THEN 'Compliant'
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age BETWEEN -1 AND 0 THEN 'At Risk'
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age > 0 THEN 'Non-Compliant'
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND d.last_status_change_age BETWEEN 0 AND 4 THEN 'Compliant'
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND d.last_status_change_age BETWEEN 5 AND 6 THEN 'At Risk'
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND (d.last_status_change_age >= 7
       OR d.last_status_change_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 15 THEN 'Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 16 AND 20 THEN 'At Risk'
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 20
       OR disposition_mod_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND d.activity_due_date_age <= -3 THEN 'Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND d.activity_due_date_age BETWEEN -2 AND 0 THEN 'At Risk'
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (d.activity_due_date_age > 0
       OR d.activity_due_date_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 3 THEN 'Compliant'
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 4 AND 7 THEN 'At Risk'
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 7
       OR disposition_mod_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 15 THEN 'Compliant'
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 16 AND 20 THEN 'At Risk'
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 20
       OR disposition_mod_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND appeal_sent_age < 60 THEN 'Compliant'
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND appeal_sent_age = 60 THEN 'At Risk'
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND (appeal_sent_age > 60
       OR appeal_sent_age IS NULL) THEN 'Non-Compliant'
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND d.activity_due_date_age <= -3 THEN 'Compliant'
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND d.activity_due_date_age BETWEEN -2 AND 0 THEN 'At Risk'
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND (d.activity_due_date_age > 0
       OR d.activity_due_date_age IS NULL) THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS post_sent_kpi,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 4
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 4
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 4
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 4
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 3
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 3
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 1
      ELSE 5
    END AS status_kpi_int,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 3
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 3
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 3
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 3
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 3
      ELSE 4
    END AS followup_kpi_int,
    appealsent_kpi_int AS appealsent_kpi_int,
    deadline_kpi_int AS deadline_kpi_int,
    CASE
      WHEN appealsent_kpi_int = 3
       OR deadline_kpi_int = 3 THEN 3
      WHEN (appealsent_kpi_int = 2
       OR deadline_kpi_int = 2)
       AND appealsent_kpi_int <> 3
       AND deadline_kpi_int <> 3 THEN 2
      WHEN (appealsent_kpi_int = 1
       OR deadline_kpi_int = 1)
       AND appealsent_kpi_int NOT IN(
        3, 2
      )
       AND deadline_kpi_int NOT IN(
        3, 2
      ) THEN 1
      WHEN appealsent_kpi_int = 5
       AND deadline_kpi_int = 5 THEN 5
      ELSE 5
    END AS appealsent_deadline_kpi_int,
    CASE
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age <= -2 THEN 1
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age BETWEEN -1 AND 0 THEN 2
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age > 0 THEN 3
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND d.last_status_change_age BETWEEN 0 AND 4 THEN 1
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND d.last_status_change_age BETWEEN 5 AND 6 THEN 2
      WHEN adjusted_current_appeal_balance < 250
       AND d.activity_due_date_age IS NULL
       AND (d.last_status_change_age >= 7
       OR d.last_status_change_age IS NULL) THEN 3
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 15 THEN 1
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 16 AND 20 THEN 2
      WHEN upper(rtrim(d.disp_code)) = 'ORNA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 20
       OR disposition_mod_age IS NULL) THEN 3
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND d.activity_due_date_age <= -3 THEN 1
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND d.activity_due_date_age BETWEEN -2 AND 0 THEN 2
      WHEN upper(rtrim(d.disp_code)) = 'OCOND'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (d.activity_due_date_age > 0
       OR disposition_mod_age IS NULL) THEN 3
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 3 THEN 1
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 4 AND 7 THEN 2
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 7
       OR disposition_mod_age IS NULL) THEN 3
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age <= 15 THEN 1
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND disposition_mod_age BETWEEN 16 AND 20 THEN 2
      WHEN upper(rtrim(d.disp_code)) = 'ORLA'
       AND upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND adjusted_current_appeal_balance >= 250
       AND (disposition_mod_age > 20
       OR disposition_mod_age IS NULL) THEN 3
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND appeal_sent_age < 60 THEN 1
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND appeal_sent_age = 60 THEN 2
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499')
       AND (appeal_sent_age > 60
       OR appeal_sent_age IS NULL) THEN 3
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND d.activity_due_date_age <= -3 THEN 1
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND d.activity_due_date_age BETWEEN -2 AND 0 THEN 2
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND d.max_seqno >= 1
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND (d.activity_due_date_age > 0
       OR d.activity_due_date_age IS NULL) THEN 3
      ELSE 5
    END AS post_sent_kpi_int,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 'Status Inconsistent with Denials'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL
       AND (upper(upper(d.status_desc)) NOT LIKE '%VALIDATED_DEN%'
       OR upper(d.status_desc) IS NULL)
       AND (upper(upper(d.status_desc)) NOT LIKE '%UNWORKED_DEN%'
       OR upper(d.status_desc) IS NULL) THEN 'Status Inconsistent with Denials'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 'Closed Appeal'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND d.close_date IS NOT NULL THEN 'Closed Appeal'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 'Unworked Denials <= 4 Days'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 4 THEN 'Unworked Denials <= 4 Days'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 'Unworked Denials > 4 Days and <= 7 Days'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 5 AND 7 THEN 'Unworked Denials > 4 Days and <= 7 Days'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Unworked Denials > 7 Days'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Unworked Denials > 7 Days'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Validated <= 180 Days Not in Dispute Project'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Validated <= 180 Days Not in Dispute Project'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Validated > 180 Days Not in Dispute Project'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND (upper(upper(d.project_name)) NOT LIKE '%DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Validated > 180 Days Not in Dispute Project'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Validated <= 180 Days in Dispute Project'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) <= 180 THEN 'Validated <= 180 Days in Dispute Project'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Validated > 180 Days in Dispute Project'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND upper(upper(d.project_name)) LIKE '%DISPUTE%'
       AND date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 'Validated > 180 Days in Dispute Project'
      ELSE CAST(NULL as STRING)
    END AS statusstratification,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 'Unworked'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 'Unworked'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 'Unworked'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 'Unworked'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Unworked'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 'Unworked'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 'Validated'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 'Validated'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 'Validated'
      ELSE 'Undefined'
    END AS followup_strat,
    CASE
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) <= 3 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) BETWEEN 4 AND 7 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 1
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%'
       AND date_diff(d.extract_date, d.last_status_change_date, DAY) > 7 THEN 1
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) < 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) = 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND date_diff(d.extract_date, d.activity_due_date, DAY) > 0
       AND d.activity_due_date IS NOT NULL THEN 2
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 2
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.activity_due_date IS NULL THEN 2
      ELSE 3
    END AS followup_strat_int,
    CASE
      WHEN upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION' THEN 'Undefined'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) < 26 THEN 'Appeal Unsent - Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) < 26 THEN 'Appeal Unsent - Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) BETWEEN 26 AND 30 THEN 'Appeal Unsent - At Risk'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) BETWEEN 26 AND 30 THEN 'Appeal Unsent - At Risk'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) > 30 THEN 'Appeal Unsent - Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.extract_date, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) > 30 THEN 'Appeal Unsent - Non-Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NOT NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.apl_sent_dt, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) <= 30 THEN 'Appeal Sent - Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NOT NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.apl_sent_dt, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) <= 30 THEN 'Appeal Sent - Compliant'
      WHEN d.payor_financial_class IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NOT NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.apl_sent_dt, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) > 30 THEN 'Appeal Sent - Non-Compliant'
      WHEN d.payor_financial_class NOT IN(
        1, 2, 3, 6
      )
       AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
       AND d.apl_sent_dt IS NOT NULL
       AND d.first_denial_date IS NOT NULL
       AND date_diff(d.apl_sent_dt, CASE
        WHEN d.seq_no = 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END, DAY) > 30 THEN 'Appeal Sent - Non-Compliant'
      ELSE 'Undefined'
    END AS appealsentstratification,
    CASE
      WHEN d.max_seq_deadline_date IS NULL THEN 'Null Deadline Date'
      WHEN d.appeal_deadline_days_remaining > 10 THEN 'Deadline Future Compliant'
      WHEN d.appeal_deadline_days_remaining < 0
       AND CASE
        WHEN d.seq_no <= 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END <= d.max_seq_deadline_date THEN CASE
        WHEN d.seq_no = 1 THEN CASE
          WHEN date_diff(d.max_seq_deadline_date, d.first_denial_date, DAY) <= 7 THEN 'Exceeded App Lvl 1 Orig <= 7 Days Before Deadline'
          WHEN date_diff(d.max_seq_deadline_date, d.first_denial_date, DAY) > 7 THEN 'Exceeded App Lvl 1 Orig 7+ Days Before Deadline'
        END
        WHEN d.seq_no <> 1 THEN 'Exceeded App Lvl 2+ Orig Before Deadline'
      END
      WHEN d.appeal_deadline_days_remaining < 0
       AND CASE
        WHEN d.seq_no <= 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END > d.max_seq_deadline_date THEN 'Exceeded Orig After Deadline'
      WHEN d.appeal_deadline_days_remaining = 0 THEN 'Due Today'
      WHEN d.appeal_deadline_days_remaining > 0
       AND d.appeal_deadline_days_remaining <= 10 THEN 'Future At Risk'
      ELSE 'Other'
    END AS deadlinestratification,
    CASE
      WHEN d.max_seq_deadline_date IS NULL THEN 1
      WHEN d.appeal_deadline_days_remaining > 10 THEN 8
      WHEN d.appeal_deadline_days_remaining < 0
       AND CASE
        WHEN d.seq_no <= 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END <= d.max_seq_deadline_date THEN CASE
        WHEN d.seq_no = 1 THEN CASE
          WHEN date_diff(d.max_seq_deadline_date, d.first_denial_date, DAY) <= 7 THEN 2
          WHEN date_diff(d.max_seq_deadline_date, d.first_denial_date, DAY) > 7 THEN 3
        END
        WHEN d.seq_no <> 1 THEN 4
      END
      WHEN d.appeal_deadline_days_remaining < 0
       AND CASE
        WHEN d.seq_no <= 1 THEN d.first_denial_date
        ELSE d.sequence_date_created
      END > d.max_seq_deadline_date THEN 5
      WHEN d.appeal_deadline_days_remaining = 0 THEN 6
      WHEN d.appeal_deadline_days_remaining > 0
       AND d.appeal_deadline_days_remaining <= 10 THEN 7
      ELSE 9
    END AS deadlinestratificationint,
    post_sent_category AS post_sent_category,
    CASE
      WHEN adjusted_current_appeal_balance < 250 THEN 1
      WHEN upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND upper(rtrim(d.disp_code)) = 'ORNA'
       AND adjusted_current_appeal_balance >= 250 THEN 2
      WHEN upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND upper(rtrim(d.disp_code)) = 'ORLA'
       AND adjusted_current_appeal_balance >= 250 THEN 3
      WHEN upper(rtrim(appealsentflag)) = 'SENT'
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND upper(rtrim(d.disp_code)) = 'OCOND'
       AND adjusted_current_appeal_balance >= 250 THEN 4
      WHEN upper(d.disp_code) IN(
        'OIWRW', 'OPAW', 'OAWA'
      )
       AND adjusted_current_appeal_balance >= 250 THEN 5
      WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
       OR upper(rtrim(dollarstrat)) = '1000-1499') THEN 6
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND (upper(rtrim(appealsentflag)) = 'SENT'
       OR upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
       AND upper(rtrim(followupdispflag)) = 'OTHER'
       AND adjusted_current_appeal_balance >= 250 THEN 7
      ELSE 8
    END AS post_sent_category_int,
    CASE
      WHEN upper(rtrim(unworked_flag)) = 'UNWORKED'
       AND upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250' THEN 'Unworked'
      WHEN upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250'
       AND upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND upper(rtrim(dollarstratfp)) <> 'VERY LOW'
       AND upper(rtrim(dollarstrat)) <> '1000-1499' THEN 'Follow-Up'
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) <> 'EXCLUDE DISP FOR EXCEPTION'
       AND upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250' THEN 'Appeal Sent/Deadline'
      WHEN upper(rtrim(post_sent_category)) <> 'UNDEFINED' THEN 'Post Appeal Sent Not In Follow-Up'
      ELSE 'Undefined'
    END AS kpi_category,
    CASE
      WHEN upper(rtrim(unworked_flag)) = 'UNWORKED'
       AND upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250' THEN 1
      WHEN upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250'
       AND upper(rtrim(followupdispflag)) = 'FOLLOWUP'
       AND upper(rtrim(dollarstratfp)) <> 'VERY LOW'
       AND upper(rtrim(dollarstrat)) <> '1000-1499' THEN 2
      WHEN upper(rtrim(unworked_flag)) = 'WORKED'
       AND upper(rtrim(appealsentflag)) = 'UNSENT'
       AND upper(rtrim(appsent_excrpt_disp_flag)) <> 'EXCLUDE DISP FOR EXCEPTION'
       AND upper(rtrim(dollarstrat)) <> 'LT 0'
       AND upper(rtrim(dollarstrat)) <> '0 - 250' THEN 3
      WHEN upper(rtrim(post_sent_category)) <> 'UNDEFINED' THEN 4
      ELSE 5
    END AS kpi_category_int,
    appealsentflag AS appealsentflag,
    followupdispflag AS followupdispflag,
    CASE
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) <= 30 THEN 1
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 31 AND 60 THEN 2
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 61 AND 90 THEN 3
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 91 AND 120 THEN 4
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 121 AND 150 THEN 5
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 151 AND 180 THEN 6
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN 7
      ELSE 8
    END AS denial_orig_age_num,
    CASE
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) <= 30 THEN '<= 30'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 31 AND 60 THEN '31-60'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 61 AND 90 THEN '61-90'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 91 AND 120 THEN '91-120'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 121 AND 150 THEN '121-150'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) BETWEEN 151 AND 180 THEN '151-180'
      WHEN date_diff(d.extract_date, d.first_denial_date, DAY) > 180 THEN '> 180'
      ELSE 'Null Age'
    END AS denial_orig_age_stra,
    adjusted_current_appeal_balance AS adjusted_current_appeal_balance,
    CASE
      WHEN adjusted_current_appeal_balance < 0 THEN 'Negative Balance'
      ELSE 'Other'
    END AS negative_curr_app_bal_flag,
    CASE
      WHEN adjusted_current_appeal_balance < 0 THEN 1
      WHEN adjusted_current_appeal_balance >= 0
       AND adjusted_current_appeal_balance < 250 THEN 2
      WHEN adjusted_current_appeal_balance >= 250
       AND adjusted_current_appeal_balance < 500 THEN 3
      WHEN adjusted_current_appeal_balance >= 500
       AND adjusted_current_appeal_balance < 750 THEN 4
      WHEN adjusted_current_appeal_balance >= 750
       AND adjusted_current_appeal_balance < 1000 THEN 5
      WHEN adjusted_current_appeal_balance >= 1000
       AND adjusted_current_appeal_balance < 1500 THEN 6
      WHEN adjusted_current_appeal_balance >= 1500
       AND adjusted_current_appeal_balance < 3000 THEN 7
      WHEN adjusted_current_appeal_balance >= 3000
       AND adjusted_current_appeal_balance < 5000 THEN 8
      WHEN adjusted_current_appeal_balance >= 5000
       AND adjusted_current_appeal_balance < 10000 THEN 9
      WHEN adjusted_current_appeal_balance >= 10000
       AND adjusted_current_appeal_balance < 25000 THEN 10
      ELSE 11
    END AS dollarstratint,
    dollarstrat AS dollarstrat,
    dollarstratfp AS dollarstratfp,
    CASE
      WHEN adjusted_current_appeal_balance < 1000 THEN 5
      WHEN adjusted_current_appeal_balance >= 1000
       AND adjusted_current_appeal_balance < 3000 THEN 4
      WHEN adjusted_current_appeal_balance >= 3000
       AND adjusted_current_appeal_balance < 10000 THEN 3
      WHEN adjusted_current_appeal_balance >= 10000
       AND adjusted_current_appeal_balance < 25000 THEN 2
      WHEN adjusted_current_appeal_balance >= 25000 THEN 1
      ELSE CAST(NULL as INT64)
    END AS dollarstratfpint,
    unworked_flag AS unworked_flag,
    CASE
      WHEN d.close_date IS NULL THEN 'Open'
      ELSE 'Closed'
    END AS open_close_flag,
    CASE
      WHEN upper(d.project_name) LIKE '%CORP%'
       AND upper(d.project_name) LIKE '%DISPUTE%' THEN 'Corp Dispute'
      WHEN upper(d.project_name) LIKE '%CORP%' THEN 'Corp Other'
      WHEN upper(d.project_name) LIKE '%MOD_POTENTIAL MODELING%' THEN 'Other - Potential Modeling Issues'
      ELSE 'Other'
    END AS project_type,
    CASE
      WHEN d.payor_due_amt = 0 THEN 'Y'
      ELSE 'N'
    END AS payor_due_amt_flag,
    discharge_age_to_denial AS discharge_age_to_denial,
    CASE
      WHEN discharge_age_to_denial <= 30 THEN 1
      WHEN discharge_age_to_denial BETWEEN 31 AND 60 THEN 2
      WHEN discharge_age_to_denial BETWEEN 61 AND 90 THEN 3
      WHEN discharge_age_to_denial BETWEEN 91 AND 120 THEN 4
      WHEN discharge_age_to_denial BETWEEN 121 AND 150 THEN 5
      WHEN discharge_age_to_denial BETWEEN 151 AND 180 THEN 6
      WHEN discharge_age_to_denial BETWEEN 181 AND 210 THEN 7
      WHEN discharge_age_to_denial BETWEEN 211 AND 240 THEN 8
      WHEN discharge_age_to_denial BETWEEN 241 AND 270 THEN 9
      WHEN discharge_age_to_denial BETWEEN 271 AND 300 THEN 10
      WHEN discharge_age_to_denial BETWEEN 301 AND 330 THEN 11
      WHEN discharge_age_to_denial BETWEEN 331 AND 360 THEN 12
      WHEN discharge_age_to_denial BETWEEN 361 AND 720 THEN 13
      WHEN discharge_age_to_denial > 720 THEN 14
      ELSE 15
    END AS discharge_age_to_denial_num,
    CASE
      WHEN discharge_age_to_denial <= 30 THEN '<= 30'
      WHEN discharge_age_to_denial BETWEEN 31 AND 60 THEN '31-60'
      WHEN discharge_age_to_denial BETWEEN 61 AND 90 THEN '61-90'
      WHEN discharge_age_to_denial BETWEEN 91 AND 120 THEN '91-120'
      WHEN discharge_age_to_denial BETWEEN 121 AND 150 THEN '121-150'
      WHEN discharge_age_to_denial BETWEEN 151 AND 180 THEN '151-180'
      WHEN discharge_age_to_denial BETWEEN 181 AND 210 THEN '181-210'
      WHEN discharge_age_to_denial BETWEEN 211 AND 240 THEN '211-240'
      WHEN discharge_age_to_denial BETWEEN 241 AND 270 THEN '241-270'
      WHEN discharge_age_to_denial BETWEEN 271 AND 300 THEN '271-300'
      WHEN discharge_age_to_denial BETWEEN 301 AND 330 THEN '301-330'
      WHEN discharge_age_to_denial BETWEEN 331 AND 360 THEN '331-360'
      WHEN discharge_age_to_denial BETWEEN 361 AND 720 THEN '361-720'
      WHEN discharge_age_to_denial > 720 THEN '>720'
      ELSE 'Null Age'
    END AS discharge_age_to_denial_strat,
    CASE
      WHEN d.appeal_sent_activity_age <= 30 THEN 1
      WHEN d.appeal_sent_activity_age BETWEEN 31 AND 60 THEN 2
      WHEN d.appeal_sent_activity_age BETWEEN 61 AND 70 THEN 3
      WHEN d.appeal_sent_activity_age BETWEEN 71 AND 90 THEN 4
      WHEN d.appeal_sent_activity_age BETWEEN 91 AND 120 THEN 5
      WHEN d.appeal_sent_activity_age BETWEEN 121 AND 150 THEN 6
      WHEN d.appeal_sent_activity_age BETWEEN 151 AND 180 THEN 7
      WHEN d.appeal_sent_activity_age BETWEEN 181 AND 210 THEN 8
      WHEN d.appeal_sent_activity_age BETWEEN 211 AND 240 THEN 9
      WHEN d.appeal_sent_activity_age BETWEEN 241 AND 270 THEN 10
      WHEN d.appeal_sent_activity_age BETWEEN 271 AND 300 THEN 11
      WHEN d.appeal_sent_activity_age BETWEEN 301 AND 330 THEN 12
      WHEN d.appeal_sent_activity_age BETWEEN 331 AND 360 THEN 13
      WHEN d.appeal_sent_activity_age BETWEEN 361 AND 720 THEN 14
      WHEN d.appeal_sent_activity_age > 720 THEN 15
      ELSE 16
    END AS appeal_sent_activity_age_num,
    CASE
      WHEN d.appeal_sent_activity_age <= 30 THEN '<= 30'
      WHEN d.appeal_sent_activity_age BETWEEN 31 AND 60 THEN '31-60'
      WHEN d.appeal_sent_activity_age BETWEEN 61 AND 70 THEN '61-70'
      WHEN d.appeal_sent_activity_age BETWEEN 71 AND 90 THEN '71-90'
      WHEN d.appeal_sent_activity_age BETWEEN 91 AND 120 THEN '91-120'
      WHEN d.appeal_sent_activity_age BETWEEN 121 AND 150 THEN '121-150'
      WHEN d.appeal_sent_activity_age BETWEEN 151 AND 180 THEN '151-180'
      WHEN d.appeal_sent_activity_age BETWEEN 181 AND 210 THEN '181-210'
      WHEN d.appeal_sent_activity_age BETWEEN 211 AND 240 THEN '211-240'
      WHEN d.appeal_sent_activity_age BETWEEN 241 AND 270 THEN '241-270'
      WHEN d.appeal_sent_activity_age BETWEEN 271 AND 300 THEN '271-300'
      WHEN d.appeal_sent_activity_age BETWEEN 301 AND 330 THEN '301-330'
      WHEN d.appeal_sent_activity_age BETWEEN 331 AND 360 THEN '331-360'
      WHEN d.appeal_sent_activity_age BETWEEN 361 AND 720 THEN '361-720'
      WHEN d.appeal_sent_activity_age > 720 THEN '>720'
      ELSE 'Null Age'
    END AS appeal_sent_activity_age_strat,
    CASE
      WHEN appeal_sent_age <= 30 THEN 1
      WHEN appeal_sent_age BETWEEN 31 AND 60 THEN 2
      WHEN appeal_sent_age BETWEEN 61 AND 70 THEN 3
      WHEN appeal_sent_age BETWEEN 71 AND 90 THEN 4
      WHEN appeal_sent_age BETWEEN 91 AND 120 THEN 5
      WHEN appeal_sent_age BETWEEN 121 AND 150 THEN 6
      WHEN appeal_sent_age BETWEEN 151 AND 180 THEN 7
      WHEN appeal_sent_age BETWEEN 181 AND 210 THEN 8
      WHEN appeal_sent_age BETWEEN 211 AND 240 THEN 9
      WHEN appeal_sent_age BETWEEN 241 AND 270 THEN 10
      WHEN appeal_sent_age BETWEEN 271 AND 300 THEN 11
      WHEN appeal_sent_age BETWEEN 301 AND 330 THEN 12
      WHEN appeal_sent_age BETWEEN 331 AND 360 THEN 13
      WHEN appeal_sent_age BETWEEN 361 AND 720 THEN 14
      WHEN appeal_sent_age > 720 THEN 15
      ELSE 16
    END AS appeal_sent_age_num,
    CASE
      WHEN appeal_sent_age <= 30 THEN '<= 30'
      WHEN appeal_sent_age BETWEEN 31 AND 60 THEN '31-60'
      WHEN appeal_sent_age BETWEEN 61 AND 70 THEN '61-70'
      WHEN appeal_sent_age BETWEEN 71 AND 90 THEN '71-90'
      WHEN appeal_sent_age BETWEEN 91 AND 120 THEN '91-120'
      WHEN appeal_sent_age BETWEEN 121 AND 150 THEN '121-150'
      WHEN appeal_sent_age BETWEEN 151 AND 180 THEN '151-180'
      WHEN appeal_sent_age BETWEEN 181 AND 210 THEN '181-210'
      WHEN appeal_sent_age BETWEEN 211 AND 240 THEN '211-240'
      WHEN appeal_sent_age BETWEEN 241 AND 270 THEN '241-270'
      WHEN appeal_sent_age BETWEEN 271 AND 300 THEN '271-300'
      WHEN appeal_sent_age BETWEEN 301 AND 330 THEN '301-330'
      WHEN appeal_sent_age BETWEEN 331 AND 360 THEN '331-360'
      WHEN appeal_sent_age BETWEEN 361 AND 720 THEN '361-720'
      WHEN appeal_sent_age > 720 THEN '>720'
      ELSE 'Null Age'
    END AS appeal_sent_age_strat,
    CASE
      WHEN d.denial_age <= 30 THEN 1
      WHEN d.denial_age BETWEEN 31 AND 60 THEN 2
      WHEN d.denial_age BETWEEN 61 AND 90 THEN 3
      WHEN d.denial_age BETWEEN 91 AND 120 THEN 4
      WHEN d.denial_age BETWEEN 121 AND 150 THEN 5
      WHEN d.denial_age BETWEEN 151 AND 180 THEN 6
      WHEN d.denial_age BETWEEN 181 AND 210 THEN 7
      WHEN d.denial_age BETWEEN 211 AND 240 THEN 8
      WHEN d.denial_age BETWEEN 241 AND 270 THEN 9
      WHEN d.denial_age BETWEEN 271 AND 300 THEN 10
      WHEN d.denial_age BETWEEN 301 AND 330 THEN 11
      WHEN d.denial_age BETWEEN 331 AND 360 THEN 12
      WHEN d.denial_age BETWEEN 361 AND 720 THEN 13
      WHEN d.denial_age > 720 THEN 14
      ELSE 15
    END AS first_denial_age_num,
    CASE
      WHEN d.denial_age <= 30 THEN '<= 30'
      WHEN d.denial_age BETWEEN 31 AND 60 THEN '31-60'
      WHEN d.denial_age BETWEEN 61 AND 90 THEN '61-90'
      WHEN d.denial_age BETWEEN 91 AND 120 THEN '91-120'
      WHEN d.denial_age BETWEEN 121 AND 150 THEN '121-150'
      WHEN d.denial_age BETWEEN 151 AND 180 THEN '151-180'
      WHEN d.denial_age BETWEEN 181 AND 210 THEN '181-210'
      WHEN d.denial_age BETWEEN 211 AND 240 THEN '211-240'
      WHEN d.denial_age BETWEEN 241 AND 270 THEN '241-270'
      WHEN d.denial_age BETWEEN 271 AND 300 THEN '271-300'
      WHEN d.denial_age BETWEEN 301 AND 330 THEN '301-330'
      WHEN d.denial_age BETWEEN 331 AND 360 THEN '331-360'
      WHEN d.denial_age BETWEEN 361 AND 720 THEN '361-720'
      WHEN d.denial_age > 720 THEN '>720'
      ELSE 'Null Age'
    END AS first_denial_age_strat,
    date_diff(d.extract_date, d.discharge_date, DAY) AS discharge_age,
    CASE
      WHEN d.latest_seq_creation_date_age <= 30 THEN 1
      WHEN d.latest_seq_creation_date_age BETWEEN 31 AND 60 THEN 2
      WHEN d.latest_seq_creation_date_age BETWEEN 61 AND 90 THEN 3
      WHEN d.latest_seq_creation_date_age BETWEEN 91 AND 120 THEN 4
      WHEN d.latest_seq_creation_date_age BETWEEN 121 AND 150 THEN 5
      WHEN d.latest_seq_creation_date_age BETWEEN 151 AND 180 THEN 6
      WHEN d.latest_seq_creation_date_age BETWEEN 181 AND 210 THEN 7
      WHEN d.latest_seq_creation_date_age BETWEEN 211 AND 240 THEN 8
      WHEN d.latest_seq_creation_date_age BETWEEN 241 AND 270 THEN 9
      WHEN d.latest_seq_creation_date_age BETWEEN 271 AND 300 THEN 10
      WHEN d.latest_seq_creation_date_age BETWEEN 301 AND 330 THEN 11
      WHEN d.latest_seq_creation_date_age BETWEEN 331 AND 360 THEN 12
      WHEN d.latest_seq_creation_date_age BETWEEN 361 AND 720 THEN 13
      WHEN d.latest_seq_creation_date_age > 720 THEN 14
      ELSE 15
    END AS latest_seq_creation_date_age_num,
    CASE
      WHEN d.latest_seq_creation_date_age <= 30 THEN '<= 30'
      WHEN d.latest_seq_creation_date_age BETWEEN 31 AND 60 THEN '31-60'
      WHEN d.latest_seq_creation_date_age BETWEEN 61 AND 90 THEN '61-90'
      WHEN d.latest_seq_creation_date_age BETWEEN 91 AND 120 THEN '91-120'
      WHEN d.latest_seq_creation_date_age BETWEEN 121 AND 150 THEN '121-150'
      WHEN d.latest_seq_creation_date_age BETWEEN 151 AND 180 THEN '151-180'
      WHEN d.latest_seq_creation_date_age BETWEEN 181 AND 210 THEN '181-210'
      WHEN d.latest_seq_creation_date_age BETWEEN 211 AND 240 THEN '211-240'
      WHEN d.latest_seq_creation_date_age BETWEEN 241 AND 270 THEN '241-270'
      WHEN d.latest_seq_creation_date_age BETWEEN 271 AND 300 THEN '271-300'
      WHEN d.latest_seq_creation_date_age BETWEEN 301 AND 330 THEN '301-330'
      WHEN d.latest_seq_creation_date_age BETWEEN 331 AND 360 THEN '331-360'
      WHEN d.latest_seq_creation_date_age BETWEEN 361 AND 720 THEN '361-720'
      WHEN d.latest_seq_creation_date_age > 720 THEN '>720'
      ELSE 'Null Age'
    END AS latest_seq_creation_date_age_strat,
    CASE
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) <= 30 THEN 1
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 31 AND 60 THEN 2
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 61 AND 90 THEN 3
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 91 AND 120 THEN 4
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 121 AND 150 THEN 5
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 151 AND 180 THEN 6
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 181 AND 210 THEN 7
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 211 AND 240 THEN 8
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 241 AND 270 THEN 9
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 271 AND 300 THEN 10
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 301 AND 330 THEN 11
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 331 AND 360 THEN 12
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 361 AND 720 THEN 13
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) > 720 THEN 14
      ELSE 15
    END AS last_reason_aging_num,
    CASE
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) <= 30 THEN '<= 30'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 31 AND 60 THEN '31-60'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 61 AND 90 THEN '61-90'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 91 AND 120 THEN '91-120'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 121 AND 150 THEN '121-150'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 151 AND 180 THEN '151-180'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 181 AND 210 THEN '181-210'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 211 AND 240 THEN '211-240'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 241 AND 270 THEN '241-270'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 271 AND 300 THEN '271-300'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 301 AND 330 THEN '301-330'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 331 AND 360 THEN '331-360'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) BETWEEN 361 AND 720 THEN '361-720'
      WHEN date_diff(d.extract_date, d.last_reason_change_date, DAY) > 720 THEN '>720'
      ELSE 'Null Age'
    END AS last_reason_aging_strat,
    disposition_mod_age AS disposition_mod_age,
    CASE
      WHEN disposition_mod_age <= 30 THEN 1
      WHEN disposition_mod_age BETWEEN 31 AND 60 THEN 2
      WHEN disposition_mod_age BETWEEN 61 AND 90 THEN 3
      WHEN disposition_mod_age BETWEEN 91 AND 120 THEN 4
      WHEN disposition_mod_age BETWEEN 121 AND 150 THEN 5
      WHEN disposition_mod_age BETWEEN 151 AND 180 THEN 6
      WHEN disposition_mod_age BETWEEN 181 AND 210 THEN 7
      WHEN disposition_mod_age BETWEEN 211 AND 240 THEN 8
      WHEN disposition_mod_age BETWEEN 241 AND 270 THEN 9
      WHEN disposition_mod_age BETWEEN 271 AND 300 THEN 10
      WHEN disposition_mod_age BETWEEN 301 AND 330 THEN 11
      WHEN disposition_mod_age BETWEEN 331 AND 360 THEN 12
      WHEN disposition_mod_age BETWEEN 361 AND 720 THEN 13
      WHEN disposition_mod_age > 720 THEN 14
      ELSE 15
    END AS disposition_mod_aging_num,
    CASE
      WHEN disposition_mod_age <= 30 THEN '<= 30'
      WHEN disposition_mod_age BETWEEN 31 AND 60 THEN '31-60'
      WHEN disposition_mod_age BETWEEN 61 AND 90 THEN '61-90'
      WHEN disposition_mod_age BETWEEN 91 AND 120 THEN '91-120'
      WHEN disposition_mod_age BETWEEN 121 AND 150 THEN '121-150'
      WHEN disposition_mod_age BETWEEN 151 AND 180 THEN '151-180'
      WHEN disposition_mod_age BETWEEN 181 AND 210 THEN '181-210'
      WHEN disposition_mod_age BETWEEN 211 AND 240 THEN '211-240'
      WHEN disposition_mod_age BETWEEN 241 AND 270 THEN '241-270'
      WHEN disposition_mod_age BETWEEN 271 AND 300 THEN '271-300'
      WHEN disposition_mod_age BETWEEN 301 AND 330 THEN '301-330'
      WHEN disposition_mod_age BETWEEN 331 AND 360 THEN '331-360'
      WHEN disposition_mod_age BETWEEN 361 AND 720 THEN '361-720'
      WHEN disposition_mod_age > 720 THEN '>720'
      ELSE 'Null Age'
    END AS disposition_mod_aging_strat,
    date_diff(d.extract_date, d.artiva_activity_due_date, DAY) AS artiva_activity_due_date_age,
    d.apl_lvl AS appeal_level,
    d.apl_sent_dt AS appeal_sent_date,
    d.prior_apl_rspn_dt AS prior_appeal_response_date,
    appeal_sent_age AS appeal_sent_age
  FROM
    {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory AS d
    LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mp ON d.mon_account_payer_id = mp.id
     AND mp.schema_id = d.schema_id
     AND mp.payer_rank = d.payer_rank
    CROSS JOIN UNNEST(ARRAY[
      date_diff(d.first_denial_date, d.discharge_date, DAY)
    ]) AS discharge_age_to_denial
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(upper(d.status_desc)) LIKE '%UNWORKED_DEN%' THEN 'Unworked'
        ELSE 'Worked'
      END
    ]) AS unworked_flag
    CROSS JOIN UNNEST(ARRAY[
      date_diff(d.extract_date, d.disposition_code_modified_date, DAY)
    ]) AS disposition_mod_age
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(upper(trim(d.disp_desc))) LIKE '%[COLL]%'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT ALJ'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT DAB'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT FI/MAC'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT QIC'
         OR upper(trim(d.disp_desc)) = 'APPEAL SENT USDC'
         OR upper(trim(d.disp_desc)) = 'INITIAL FOLLOW UP'
         OR upper(trim(d.disp_desc)) = 'CONTINUED FOLLOW UP'
         OR upper(trim(d.disp_desc)) = 'RECEIPT CONFIRMATION NEEDED'
         OR upper(trim(d.disp_desc)) = 'CONFIRM RECEIPT OF APPEAL'
         OR upper(trim(d.disp_desc)) = 'SUBMITTED TO IRF'
         OR upper(trim(d.disp_desc)) = 'SUBSEQUENT FOLLOW UP'
         OR upper(trim(d.disp_desc)) = 'SUBMITTED ON PAYOR PACKAGE'
         OR upper(trim(d.disp_desc)) = 'PROMISE TO PAY'
         OR upper(trim(d.disp_desc)) = 'PENDING-PROMISE TO PAY'
         OR upper(trim(d.disp_desc)) = 'SUBMITTED ON PAYOR PACKAGE'
         OR upper(trim(d.disp_desc)) = 'TRANSFER TO LEGAL'
         OR upper(trim(d.disp_desc)) = 'TURN OVER TO ATTORNEY'
         OR upper(trim(d.disp_desc)) = 'ESCALATE - DISPUTE RESOLUTION TEAM'
         OR upper(trim(d.disp_desc)) = 'CONFIRM RECEIPT OF APPEAL'
         OR upper(trim(d.disp_desc)) = 'PENDING - PROMISE TO PAY'
         OR upper(trim(d.disp_desc)) = 'PENDING MANAGER REVIEW'
         OR upper(trim(d.disp_desc)) = 'SUBMITTED ON PAYOR PKG'
         OR upper(trim(d.disp_desc)) = 'EXPAPP SHIPPED'
         OR upper(trim(d.disp_desc)) = 'ESCALATE - SSC ATTORNEY' THEN 'FollowUp'
        ELSE 'Other'
      END
    ]) AS followupdispflag
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN (d.seq_no IS NULL
         OR d.seq_no = 0)
         AND d.current_appeal_balance = 0
         OR d.current_appeal_balance IS NULL THEN d.payor_due_amt
        ELSE d.current_appeal_balance
      END
    ]) AS adjusted_current_appeal_balance
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN adjusted_current_appeal_balance < 0 THEN 'LT 0'
        WHEN adjusted_current_appeal_balance >= 0
         AND adjusted_current_appeal_balance < 250 THEN '0 - 250'
        WHEN adjusted_current_appeal_balance >= 250
         AND adjusted_current_appeal_balance < 500 THEN '250-499'
        WHEN adjusted_current_appeal_balance >= 500
         AND adjusted_current_appeal_balance < 750 THEN '500-749'
        WHEN adjusted_current_appeal_balance >= 750
         AND adjusted_current_appeal_balance < 1000 THEN '750-999'
        WHEN adjusted_current_appeal_balance >= 1000
         AND adjusted_current_appeal_balance < 1500 THEN '1000-1499'
        WHEN adjusted_current_appeal_balance >= 1500
         AND adjusted_current_appeal_balance < 3000 THEN '1500-2999'
        WHEN adjusted_current_appeal_balance >= 3000
         AND adjusted_current_appeal_balance < 5000 THEN '3000-4999'
        WHEN adjusted_current_appeal_balance >= 5000
         AND adjusted_current_appeal_balance < 10000 THEN '5000-9999'
        WHEN adjusted_current_appeal_balance >= 10000
         AND adjusted_current_appeal_balance < 25000 THEN '10000-24999'
        ELSE 'GT 25000'
      END
    ]) AS dollarstrat
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN adjusted_current_appeal_balance < 1000 THEN 'Very Low'
        WHEN adjusted_current_appeal_balance >= 1000
         AND adjusted_current_appeal_balance < 3000 THEN 'Low'
        WHEN adjusted_current_appeal_balance >= 3000
         AND adjusted_current_appeal_balance < 10000 THEN 'Mid'
        WHEN adjusted_current_appeal_balance >= 10000
         AND adjusted_current_appeal_balance < 25000 THEN 'High'
        WHEN adjusted_current_appeal_balance >= 25000 THEN 'Top'
        ELSE CAST(NULL as STRING)
      END
    ]) AS dollarstratfp
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining > 3 THEN 'Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining > 3 THEN 'Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining BETWEEN 0 AND 3 THEN 'At Risk'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining BETWEEN 0 AND 3 THEN 'At Risk'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining < 0 THEN 'Non-Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining < 0 THEN 'Non-Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date >= d.apl_sent_dt THEN 'Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date >= d.apl_sent_dt THEN 'Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date < d.apl_sent_dt THEN 'Non-Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date < d.apl_sent_dt THEN 'Non-Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) <= 7 THEN 'Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) <= 7 THEN 'Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) BETWEEN 8 AND 30 THEN 'At Risk'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) BETWEEN 8 AND 30 THEN 'At Risk'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) > 30 THEN 'Non-Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) > 30 THEN 'Non-Compliant'
        ELSE 'Undefined'
      END
    ]) AS deadline_kpi
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining > 3 THEN 1
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining > 3 THEN 1
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining BETWEEN 0 AND 3 THEN 2
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining BETWEEN 0 AND 3 THEN 2
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining < 0 THEN 3
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.appeal_deadline_days_remaining < 0 THEN 3
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date >= d.apl_sent_dt THEN 1
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date >= d.apl_sent_dt THEN 1
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date < d.apl_sent_dt THEN 3
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.max_seq_deadline_date < d.apl_sent_dt THEN 3
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) <= 7 THEN 1
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) <= 7 THEN 1
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) BETWEEN 8 AND 30 THEN 2
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) BETWEEN 8 AND 30 THEN 2
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) > 30 THEN 3
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.max_seq_deadline_date IS NULL
         AND d.apl_sent_dt IS NULL
         AND date_diff(d.extract_date, d.sequence_date_created, DAY) > 30 THEN 3
        ELSE 5
      END
    ]) AS deadline_kpi_int
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN d.apl_sent_dt IS NULL THEN 'Unsent'
        WHEN d.apl_sent_dt < d.sequence_date_created THEN 'Unsent'
        WHEN d.apl_sent_dt >= d.sequence_date_created THEN 'Sent'
        ELSE 'Other'
      END
    ]) AS appealsentflag
    CROSS JOIN UNNEST(ARRAY[
      date_diff(current_date('US/Central'), d.apl_sent_dt, DAY)
    ]) AS appeal_sent_age
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(trim(d.disp_desc)) IN(
          'CONTINUED FOLLOW UP', 'SUBSEQUENT FOLLOW UP', 'IN WRITE OFF REVIEW', 'IN WRITE-OFF REVIEW', 'INITIAL FOLLOW UP', 'BELOW THRESHOLD', 'SETTLEMENT SUBMITTED', 'RECEIPT CONFIRMATION NEEDED', 'ESCALATE - DISPUTE RESOLUTION TEAM', 'ESCALATE - SSC ATTORNEY', 'EXPAPP SHIPPED', 'IN ARBITRATION', 'INACTIVE - NOT TRUE DENIAL', 'INACTIVE-NOT TRUE DENIAL', 'AUTOMATION ERROR - BELOW THRESHOLD', 'NOT ABLE TO APPEAL', 'NOT APPEALED', 'PAYER VALIDATION OF RECEIPT NEEDED', 'NOT APPEALED - NONHCA', 'NOT APPEALED - FEDERAL, STATE OR LOCAL REGULATION', 'PROMISE TO PAY', 'REBILLED', 'SUBMITTED ON PAYOR PACKAGE', 'SUCCESSFUL APPEAL - SETTLEMENT', 'TURN OVER TO VENDOR', 'TIMELY FILING APPEAL', 'WITHDRAWAL REQUESTED'
        )
         OR upper(trim(d.disp_desc)) LIKE '%APPEAL SENT%'
         OR upper(trim(d.disp_desc)) LIKE '%OVERTURNED%'
         OR upper(trim(d.disp_desc)) LIKE '%UPHELD%'
         OR upper(trim(d.disp_desc)) LIKE '%[COLL]%' THEN 'Exclude Disp for Exception'
        ELSE 'Appeal Sent KPI Disp'
      END
    ]) AS appsent_excrpt_disp_flag
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION' THEN 5
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) < 26 THEN 1
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) < 26 THEN 1
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) BETWEEN 26 AND 30 THEN 2
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) BETWEEN 26 AND 30 THEN 2
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 3
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 3
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) <= 30 THEN 1
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) <= 30 THEN 1
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 3
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 3
        ELSE 5
      END
    ]) AS appealsent_kpi_int
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN adjusted_current_appeal_balance < 250 THEN 'LT $250'
        WHEN upper(rtrim(appealsentflag)) = 'SENT'
         AND upper(rtrim(followupdispflag)) = 'OTHER'
         AND upper(rtrim(d.disp_code)) = 'ORNA'
         AND adjusted_current_appeal_balance >= 250 THEN 'Review Needed by Appeals'
        WHEN upper(rtrim(appealsentflag)) = 'SENT'
         AND upper(rtrim(followupdispflag)) = 'OTHER'
         AND upper(rtrim(d.disp_code)) = 'ORLA'
         AND adjusted_current_appeal_balance >= 250 THEN 'Resubmit Level of Appeal'
        WHEN upper(rtrim(appealsentflag)) = 'SENT'
         AND upper(rtrim(followupdispflag)) = 'OTHER'
         AND upper(rtrim(d.disp_code)) = 'OCOND'
         AND adjusted_current_appeal_balance >= 250 THEN 'Contract Dispute'
        WHEN upper(d.disp_code) IN(
          'OIWRW', 'OPAW', 'OAWA'
        )
         AND adjusted_current_appeal_balance >= 250 THEN 'In Write Off Review'
        WHEN upper(rtrim(followupdispflag)) = 'FOLLOWUP'
         AND (upper(rtrim(dollarstratfp)) = 'VERY LOW'
         OR upper(rtrim(dollarstrat)) = '1000-1499') THEN '< $1500 Appeals Sent'
        WHEN upper(rtrim(unworked_flag)) = 'WORKED'
         AND (upper(rtrim(appealsentflag)) = 'SENT'
         OR upper(rtrim(appealsentflag)) = 'UNSENT'
         AND upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION')
         AND upper(rtrim(followupdispflag)) = 'OTHER'
         AND adjusted_current_appeal_balance >= 250 THEN 'All Other Non-Follow-Up Dispositions'
        ELSE 'Undefined'
      END
    ]) AS post_sent_category
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN upper(rtrim(appsent_excrpt_disp_flag)) = 'EXCLUDE DISP FOR EXCEPTION' THEN 'Undefined'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) < 26 THEN 'Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) < 26 THEN 'Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) BETWEEN 26 AND 30 THEN 'At Risk'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) BETWEEN 26 AND 30 THEN 'At Risk'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 'Non-Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.extract_date, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 'Non-Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) <= 30 THEN 'Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) <= 30 THEN 'Compliant'
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 'Non-Compliant'
        WHEN d.payor_financial_class NOT IN(
          1, 2, 3, 6
        )
         AND upper(upper(d.status_desc)) LIKE '%VALIDATED_DEN%'
         AND d.apl_sent_dt IS NOT NULL
         AND d.first_denial_date IS NOT NULL
         AND date_diff(d.apl_sent_dt, CASE
          WHEN d.seq_no = 1 THEN d.first_denial_date
          ELSE d.sequence_date_created
        END, DAY) > 30 THEN 'Non-Compliant'
        ELSE 'Undefined'
      END
    ]) AS appealsent_kpi
;