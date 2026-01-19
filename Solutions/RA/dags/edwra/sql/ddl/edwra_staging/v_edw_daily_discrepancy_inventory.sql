CREATE OR REPLACE VIEW {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_discrepancy_inventory(schema_id, mon_account_payer_id, coid, account_no, payer_rank, ssc_name, facility_name, sec_establishment_id, unit_num, rate_schedule_name, rate_schedule_eff_begin_date, rate_schedule_eff_end_date, patient_name, patient_dob, iplan_id, insurance_provider_name, payer_group_name, billing_name, billing_contact_name, authorization_code, payer_patient_id, pa_financial_class, payor_financial_class, reason_code, reason_code_2, reason_code_3, reason_code_4, billing_status, pa_service_code, pa_account_status, cc_patient_type, pa_discharge_status, pa_patient_type, cancel_bill_ind, admit_source, pa_drg, remit_drg, attending_physician_id, attending_physician_name, service_date_begin, discharge_date, comparison_method, project_name, work_queue_name, status_category_desc, status_desc, status_phase_desc, calc_date, overpayment_date, overpayment_age, underpayment_date, underpayment_age, non_fin_disc_date, non_fin_disc_age, variance_creation_date, variance_creation_age, variance_resolution_date, variance_resolution_age, first_activity_create_date, last_activity_completion_date, last_activity_completion_age, last_reason_change_date, last_reason_change_date_2, last_reason_change_date_3, last_reason_change_date_4, last_status_change_date, last_project_change_date, last_owner_change_date, activity_due_date, activity_description, activity_subject, activity_owner, crt_placed_activity_date, total_charges, pa_actual_los, total_billed_charges, total_expected_payment, total_expected_adjustment, total_pt_responsibility, total_variance_adjustment, total_payments, total_denial_amount, payor_due_amount, ar_amount, pa_total_account_bal, user_completed_activity_date, user_completed_activity_age, user_completed_activity_subj, user_completed_activity_desc, user_completed_activity_ownr, valid_overpymnt_activity_dt, valid_overpymnt_activity_age, valid_overpymnt_activity_subj, valid_overpymnt_activity_desc, valid_overpymnt_activity_ownr, valid_underpymnt_activity_date, valid_underpymnt_activity_age, valid_underpymnt_activity_subj, valid_underpymnt_activity_desc, valid_underpymnt_activity_ownr, validation_grp, discrepancy_group, credit, payer_category, model_issue, credit_balance_age, extract_date, collection_amt, collection_date, source_system_code, max_type5_trans_dt, dw_last_update_date, project_type, potential_modeling_issues_flag, reason_code_assign_age, reason_code_assign_age_strat_id, reason_code_assign_age_strat, overpayment_age_strat_id, overpayment_age_strat, underpayment_age_strat_id, underpayment_age_strat, finclass_flag, opymtagingkpi, opymtagingint, opymtagingstrat, opymtsoxkpi, opymtsoxint, opymtsoxstrat, opymtactivitykpi, opymtactivityint, opymtactivitystrat, upymtagingkpi, upymtagingint, upymtagingstrat, upymtactivitykpi, upymtactivityint, upymtactivitystrat, opymt_unworked_flag, upymt_unworked_flag, opymt_unvalidated_flag, upymt_unvalidated_flag, followupflag, dollarstratint, dollarstrat, dollarstratfp, dollarstratfpint, disc_payment_type, last_reason_aging_num, last_reason_aging_strat, discharge_age_to_discrep, discharge_age_to_discrep_num, discharge_age_to_discrep_strat, discharge_age, crt_flag, exrpt_partchgprcdflag, status_kpi, activity_kpi, stratification) AS SELECT
    d.schema_id,
    d.mon_account_payer_id,
    d.coid,
    d.account_no,
    d.payer_rank,
    d.ssc_name,
    d.facility_name,
    d.sec_establishment_id,
    d.unit_num,
    d.rate_schedule_name,
    d.rate_schedule_eff_begin_date,
    d.rate_schedule_eff_end_date,
    d.patient_name,
    d.patient_dob,
    d.iplan_id,
    d.insurance_provider_name,
    d.payer_group_name,
    d.billing_name,
    d.billing_contact_name,
    d.authorization_code,
    d.payer_patient_id,
    d.pa_financial_class,
    d.payor_financial_class,
    d.reason_code,
    d.reason_code_2,
    d.reason_code_3,
    d.reason_code_4,
    d.billing_status,
    d.pa_service_code,
    d.pa_account_status,
    d.cc_patient_type,
    d.pa_discharge_status,
    d.pa_patient_type,
    d.cancel_bill_ind,
    d.admit_source,
    d.pa_drg,
    d.remit_drg_code AS remit_drg,
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
    d.overpayment_date,
    d.overpayment_age,
    d.underpayment_date,
    d.underpayment_age,
    d.non_fin_disc_date,
    d.non_fin_disc_age,
    d.variance_creation_date,
    d.variance_creation_age,
    d.variance_resolution_date,
    d.variance_resolution_age,
    d.first_activity_create_date,
    d.last_activity_completion_date,
    d.last_activity_completion_age,
    d.last_reason_change_date,
    d.last_reason_change_date_2,
    d.last_reason_change_date_3,
    d.last_reason_change_date_4,
    d.last_status_change_date,
    d.last_project_change_date,
    d.last_owner_change_date,
    d.activity_due_date,
    d.activity_description,
    d.activity_subject,
    d.activity_owner,
    d.crt_placed_activity_date,
    d.total_charges,
    d.pa_actual_los,
    d.total_billed_charges,
    d.total_expected_payment,
    d.total_expected_adjustment,
    d.total_pt_responsibility,
    d.total_variance_adjustment,
    d.total_payments,
    d.total_denial_amount,
    d.payor_due_amount,
    d.ar_amount,
    d.pa_total_account_bal,
    d.user_completed_activity_date,
    d.user_completed_activity_age,
    d.user_completed_activity_subj,
    d.user_completed_activity_desc,
    d.user_completed_activity_ownr,
    d.valid_overpymnt_activity_dt,
    d.valid_overpymnt_activity_age,
    d.valid_overpymnt_activity_subj,
    d.valid_overpymnt_activity_desc,
    d.valid_overpymnt_activity_ownr,
    d.valid_underpymnt_activity_date,
    d.valid_underpymnt_activity_age,
    d.valid_underpymnt_activity_subj,
    d.valid_underpymnt_activity_desc,
    d.valid_underpymnt_activity_ownr,
    d.validation_grp,
    CASE
      WHEN upper(d.status_desc) LIKE '%UNREVIEWED%' THEN 'Unreviewed'
      WHEN upper(d.status_phase_desc) LIKE '%NON FINANCIAL DISCREPANCY%' THEN 'Non Financial Discrepancy'
      ELSE d.discrepancy_group
    END AS discrepancy_group,
    d.credit,
    d.payer_category,
    d.model_issue,
    d.credit_balance_age,
    d.extract_date,
    d.collection_amt,
    d.collection_date,
    d.source_system_code,
    d.max_type5_trans_dt,
    d.dw_last_update_date,
    CASE
      WHEN upper(d.project_name) LIKE '%CORP%'
       AND upper(d.project_name) LIKE '%DISPUTE%' THEN 'Corp Dispute'
      WHEN upper(d.project_name) LIKE '%CORP%' THEN 'Corp Other'
      WHEN upper(d.project_name) LIKE '%MOD_POTENTIAL MODELING%' THEN 'Other - Potential Modeling Issues'
      ELSE 'Other'
    END AS project_type,
    CASE
      WHEN (d.project_name IS NULL
       OR trim(d.project_name) = ''
       OR upper(d.project_name) LIKE 'PSU_UND_PLACED%'
       OR upper(d.project_name) LIKE 'PSU_DEN_PLACED%')
       AND upper(d.reason_code) LIKE '%POTENTIAL MODELING%' THEN 'Unconfirmed'
      WHEN upper(d.project_name) LIKE '%MOD_POTENTIAL MODELING%'
       AND upper(d.reason_code) LIKE '%REEVALUATE%' THEN 'Modeling Reevaluate'
      WHEN upper(d.project_name) LIKE '%MOD_POTENTIAL MODELING%' THEN 'Confirmed'
      ELSE 'Other'
    END AS potential_modeling_issues_flag,
    reason_code_assign_age AS reason_code_assign_age,
    CASE
      WHEN d.last_reason_change_date IS NULL THEN 1
      WHEN reason_code_assign_age <= 7 THEN 2
      WHEN reason_code_assign_age >= 8
       AND reason_code_assign_age <= 30 THEN 3
      WHEN reason_code_assign_age >= 31
       AND reason_code_assign_age <= 60 THEN 4
      WHEN reason_code_assign_age >= 61
       AND reason_code_assign_age <= 90 THEN 5
      WHEN reason_code_assign_age >= 91 THEN 6
    END AS reason_code_assign_age_strat_id,
    CASE
      WHEN reason_code_assign_age IS NULL THEN 'Uncoded'
      WHEN reason_code_assign_age <= 7 THEN '0-7 Days'
      WHEN reason_code_assign_age >= 8
       AND reason_code_assign_age <= 30 THEN '8-30 Days'
      WHEN reason_code_assign_age >= 31
       AND reason_code_assign_age <= 60 THEN '31-60 Days'
      WHEN reason_code_assign_age >= 61
       AND reason_code_assign_age <= 90 THEN '61-90 Days'
      WHEN reason_code_assign_age >= 91 THEN '> 91 Days'
    END AS reason_code_assign_age_strat,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) <> 'OVERPAYMENT' THEN 10
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 7 THEN 1
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 8
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 30 THEN 2
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 31
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60 THEN 3
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 61
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 90 THEN 4
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 91
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 5
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 121
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 150 THEN 6
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 151
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 180 THEN 7
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 181
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 360 THEN 8
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 360 THEN 9
      ELSE 10
    END AS overpayment_age_strat_id,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) <> 'OVERPAYMENT' THEN 'No Strat'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 7 THEN '0-7 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 8
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 30 THEN '8-30 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 31
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60 THEN '31-60 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 61
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 90 THEN '61-90 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 91
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN '91-120 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 121
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 150 THEN '121-150 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 151
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 180 THEN '151-180 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END >= 181
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 360 THEN '180-360 Days'
      WHEN CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 360 THEN '> 360 Days'
      ELSE 'Null Age'
    END AS overpayment_age_strat,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) <> 'UNDERPAYMENT' THEN 10
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 7 THEN 1
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 8
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 30 THEN 2
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 31
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 3
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 61
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 90 THEN 4
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 91
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 120 THEN 5
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 121
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 150 THEN 6
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 151
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 7
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 181
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 360 THEN 8
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 9
      ELSE 10
    END AS underpayment_age_strat_id,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) <> 'UNDERPAYMENT' THEN 'No Strat'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 7 THEN '0-7 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 8
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 30 THEN '8-30 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 31
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN '31-60 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 61
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 90 THEN '61-90 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 91
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 120 THEN '91-120 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 121
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 150 THEN '121-150 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 151
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN '151-180 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END >= 181
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 360 THEN '181-360 Days'
      WHEN CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN '> 360 Days'
      ELSE 'Null Age'
    END AS underpayment_age_strat,
    finclass_flag AS finclass_flag,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS opymtagingkpi,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 3
      ELSE 5
    END AS opymtagingint,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Unvalidated <= 45 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Unvalidated <= 45 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'Unvalidated > 45 Days + <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'Unvalidated > 45 Days + <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Potential Overpayment <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Potential Overpayment <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Potential Overpayment > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Potential Overpayment > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Potential Overpayment > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Potential Overpayment > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Validated w/o CRT <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Validated w/o CRT > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Validated w/o CRT > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Validated Not in Appropriate Balance <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'Validated Not in Appropriate Balance > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Validated Not in Appropriate Balance > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Validated w/ CRT <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Validated w/ CRT > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Validated w/ CRT > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Validated in Appropriate Balance <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'Validated in Appropriate Balance > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Validated in Appropriate Balance > 30 Days'
      ELSE 'Undefined'
    END AS opymtagingstrat,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Non-Compliant (SOX)'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Non-Compliant (SOX)'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Non-Compliant (SOX)'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Non-Compliant (SOX)'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Compliant'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Compliant'
      ELSE 'Undefined'
    END AS opymtsoxkpi,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 3
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 1
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 1
      ELSE 5
    END AS opymtsoxint,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Uncoded <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Uncoded <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Uncoded > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Uncoded > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Unvalidated <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Unvalidated <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Unvalidated > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Unvalidated > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment w/o CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment Not in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment w/ CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment w/o CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment Not in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment w/ CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated w/o CRT <= 120 Days'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated Not in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated w/o CRT > 120 Days'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated Not in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated with CRT <= 120 Days'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated with CRT > 120 Days'
      WHEN /*and d.Reason_Code is not null*/ upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated in Appropriate Balance > 120 Days'
      ELSE 'Undefined'
    END AS opymtsoxstrat,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS opymtactivitykpi,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 3
      ELSE 5
    END AS opymtactivityint,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Non-Financial <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Financial > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Potential Overpayment - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Potential Overpayment - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      ELSE 'Undefined'
    END AS opymtactivitystrat,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 6 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 6 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END = 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END = 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Approaching Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Approaching Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 181 AND 360 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND upper(upper(d.project_name)) LIKE '%CORP_DISPUTE%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND (upper(upper(d.project_name)) NOT LIKE '%CORP_DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'At Risk'
      ELSE 'Undefined'
    END AS upymtagingkpi,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 5
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 6 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 6 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END = 7 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END = 7 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 181 AND 360 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND upper(upper(d.project_name)) LIKE '%CORP_DISPUTE%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND (upper(upper(d.project_name)) NOT LIKE '%CORP_DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 2
      ELSE 6
    END AS upymtagingint,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Uncoded > 7 Days + <= 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Uncoded > 7 Days + <= 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Uncoded > 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Uncoded > 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Unvalidated <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Unvalidated <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Validated <= 180 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 181 AND 360 THEN 'Validated > 180 Days + <= 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Validated > 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Validated > 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Validated <= 180 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND upper(upper(d.project_name)) LIKE '%CORP_DISPUTE%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Validated > 180 Days (in Corp Dispute)'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND (upper(upper(d.project_name)) NOT LIKE '%CORP_DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Validated > 180 Days (Not in Corp Dispute)'
      ELSE 'Undefined'
    END AS upymtagingstrat,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS upymtactivitykpi,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 4
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 1
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 2
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 3
      ELSE 5
    END AS upymtactivityint,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Non-Financial <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Financial > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Uncoded > 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Uncoded > 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      ELSE 'Undefined'
    END AS upymtactivitystrat,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) = 'OVERPAYMENT'
       AND d.reason_code IS NULL THEN 'Unworked'
      ELSE 'Worked'
    END AS opymt_unworked_flag,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) = 'UNDERPAYMENT'
       AND d.reason_code IS NULL THEN 'Unworked'
      ELSE 'Worked'
    END AS upymt_unworked_flag,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) = 'OVERPAYMENT'
       AND upper(upper(d.reason_code)) LIKE '%{V}%' THEN 'Validated'
      WHEN d.reason_code IS NULL THEN 'Unworked'
      ELSE 'Unvalidated'
    END AS opymt_unvalidated_flag,
    CASE
      WHEN upper(rtrim(d.discrepancy_group)) = 'UNDERPAYMENT'
       AND upper(upper(d.reason_code)) LIKE '%{V}%' THEN 'Validated'
      WHEN d.reason_code IS NULL THEN 'Unworked'
      ELSE 'Unvalidated'
    END AS upymt_unvalidated_flag,
    CASE
      WHEN DATE(d.extract_date) = (
        SELECT
            DATE(max(edw_daily_discrepancy.extract_date)) AS max_extract_date
          FROM
            {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_discrepancy
      )
       AND (upper(d.model_issue) NOT LIKE 'CONFIRMED MODELING%'
       OR d.model_issue IS NULL)
       AND upper(rtrim(d.status_phase_desc)) = 'UNDERPAYMENT'
       AND upper(rtrim(d.status_desc)) = 'VALIDATED_UND_AA_[ACT]'
       AND (upper(rtrim(d.reason_code)) NOT IN(
        'UNSOLICITED REFUND REQUEST_{V}_[1]', 'VBA/READMIT VARIANCE_{V}_[15]', 'MEDICARE DEPENDENT HOSPITAL MODEL ISSUE_{V}_[15]', 'RESOLUTION DEADLINE EXCEEDED_{V}_[15]', 'RESOLUTION DEADLINE EXCEEDED_{V}_[7]', 'PATIENT STATUS CHANGE_{V}_[15]', 'MODIFIER ISSUE_{V}_[15]', 'SPLIT BILL_{V}_[15]'
      )
       OR d.reason_code IS NULL)
       AND (upper(d.reason_code) NOT LIKE '%BANKRUPT%'
       OR d.reason_code IS NULL)
       AND (upper(d.project_name) NOT LIKE '%BANKRUPT%'
       OR d.project_name IS NULL)
       AND (upper(d.project_name) NOT LIKE '%CORP%'
       OR d.project_name IS NULL)
       AND (upper(d.project_name) NOT LIKE '%TRIAG%'
       OR d.project_name IS NULL)
       AND (upper(rtrim(d.project_name)) NOT IN(
        'TAMP_UND_POTENTIAL EFFORTS EXHAUSTED', 'TAMP_UND_OUTSOURCED TO VENDOR'
      )
       OR d.project_name IS NULL)
       AND d.payer_rank = 1
       AND d.payor_due_amount >= 250 THEN 'FollowUp'
      ELSE 'Other'
    END AS followupflag,
    CASE
      WHEN abs(d.payor_due_amount) < 250 THEN 1
      WHEN abs(d.payor_due_amount) >= 250
       AND abs(d.payor_due_amount) < 500 THEN 2
      WHEN abs(d.payor_due_amount) >= 500
       AND abs(d.payor_due_amount) < 750 THEN 3
      WHEN abs(d.payor_due_amount) >= 750
       AND abs(d.payor_due_amount) < 1000 THEN 4
      WHEN abs(d.payor_due_amount) >= 1000
       AND abs(d.payor_due_amount) < 1500 THEN 5
      WHEN abs(d.payor_due_amount) >= 1500
       AND abs(d.payor_due_amount) < 2000 THEN 6
      WHEN abs(d.payor_due_amount) >= 2000
       AND abs(d.payor_due_amount) < 3000 THEN 7
      WHEN abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 5000 THEN 8
      WHEN abs(d.payor_due_amount) >= 5000
       AND abs(d.payor_due_amount) < 10000 THEN 9
      WHEN abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 25000 THEN 10
      WHEN abs(d.payor_due_amount) >= 25000
       AND abs(d.payor_due_amount) < 50000 THEN 11
      WHEN abs(d.payor_due_amount) >= 50000
       AND abs(d.payor_due_amount) < 75000 THEN 12
      WHEN abs(d.payor_due_amount) >= 75000
       AND abs(d.payor_due_amount) < 100000 THEN 13
      ELSE 14
    END AS dollarstratint,
    CASE
      WHEN abs(d.payor_due_amount) < 250 THEN 'LT 250'
      WHEN abs(d.payor_due_amount) >= 250
       AND abs(d.payor_due_amount) < 500 THEN '250-499'
      WHEN abs(d.payor_due_amount) >= 500
       AND abs(d.payor_due_amount) < 750 THEN '500-749'
      WHEN abs(d.payor_due_amount) >= 750
       AND abs(d.payor_due_amount) < 1000 THEN '750-999'
      WHEN abs(d.payor_due_amount) >= 1000
       AND abs(d.payor_due_amount) < 1500 THEN '1000-1499'
      WHEN abs(d.payor_due_amount) >= 1500
       AND abs(d.payor_due_amount) < 2000 THEN '1500-1999'
      WHEN abs(d.payor_due_amount) >= 2000
       AND abs(d.payor_due_amount) < 3000 THEN '2000-2999'
      WHEN abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 5000 THEN '3000-4999'
      WHEN abs(d.payor_due_amount) >= 5000
       AND abs(d.payor_due_amount) < 10000 THEN '5000-9999'
      WHEN abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 25000 THEN '10000-24999'
      WHEN abs(d.payor_due_amount) >= 25000
       AND abs(d.payor_due_amount) < 50000 THEN '25000-49999'
      WHEN abs(d.payor_due_amount) >= 50000
       AND abs(d.payor_due_amount) < 75000 THEN '50000-74999'
      WHEN abs(d.payor_due_amount) >= 75000
       AND abs(d.payor_due_amount) < 100000 THEN '75000-100000'
      ELSE 'GT 100000'
    END AS dollarstrat,
    CASE
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND abs(d.payor_due_amount) < 1000 THEN 'Very Low'
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 1000
       AND abs(d.payor_due_amount) < 3000) THEN 'Low'
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 10000) THEN 'Mid'
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 25000) THEN 'High'
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND abs(d.payor_due_amount) >= 25000 THEN 'Top'
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND abs(d.payor_due_amount) < 3000 THEN 'Very Low'
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 5000) THEN 'Low'
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 5000
       AND abs(d.payor_due_amount) < 10000) THEN 'Mid'
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 50000) THEN 'High'
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND abs(d.payor_due_amount) >= 50000 THEN 'Top'
      ELSE NULL
    END AS dollarstratfp,
    CASE
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND abs(d.payor_due_amount) < 1000 THEN 5
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 1000
       AND abs(d.payor_due_amount) < 3000) THEN 4
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 10000) THEN 3
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND (abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 25000) THEN 2
      WHEN upper(rtrim(org.company_code)) <> 'H'
       AND abs(d.payor_due_amount) >= 25000 THEN 1
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND abs(d.payor_due_amount) < 3000 THEN 5
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 3000
       AND abs(d.payor_due_amount) < 5000) THEN 4
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 5000
       AND abs(d.payor_due_amount) < 10000) THEN 3
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND (abs(d.payor_due_amount) >= 10000
       AND abs(d.payor_due_amount) < 50000) THEN 2
      WHEN upper(rtrim(org.company_code)) = 'H'
       AND abs(d.payor_due_amount) >= 50000 THEN 1
      ELSE NULL
    END AS dollarstratfpint,
    CASE
      WHEN upper(d.reason_code) LIKE '%{V}%' THEN 'Payment Type'
      ELSE 'Non-Payment Type'
    END AS disc_payment_type,
    CASE
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 30 THEN 1
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 31 AND 60 THEN 2
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 61 AND 90 THEN 3
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 91 AND 120 THEN 4
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 121 AND 150 THEN 5
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 151 AND 180 THEN 6
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 181 AND 210 THEN 7
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 211 AND 240 THEN 8
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 241 AND 270 THEN 9
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 271 AND 300 THEN 10
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 301 AND 330 THEN 11
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 331 AND 360 THEN 12
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 361 AND 720 THEN 13
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 720 THEN 14
      ELSE 15
    END AS last_reason_aging_num,
    CASE
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 30 THEN '<= 30'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 31 AND 60 THEN '31-60'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 61 AND 90 THEN '61-90'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 91 AND 120 THEN '91-120'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 121 AND 150 THEN '121-150'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 151 AND 180 THEN '151-180'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 181 AND 210 THEN '181-210'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 211 AND 240 THEN '211-240'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 241 AND 270 THEN '241-270'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 271 AND 300 THEN '271-300'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 301 AND 330 THEN '301-330'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 331 AND 360 THEN '331-360'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 361 AND 720 THEN '361-720'
      WHEN date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 720 THEN '>720'
      ELSE 'Null Age'
    END AS last_reason_aging_strat,
    discharge_age_to_discrep AS discharge_age_to_discrep,
    CASE
      WHEN discharge_age_to_discrep <= 30 THEN 1
      WHEN discharge_age_to_discrep BETWEEN 31 AND 60 THEN 2
      WHEN discharge_age_to_discrep BETWEEN 61 AND 90 THEN 3
      WHEN discharge_age_to_discrep BETWEEN 91 AND 120 THEN 4
      WHEN discharge_age_to_discrep BETWEEN 121 AND 150 THEN 5
      WHEN discharge_age_to_discrep BETWEEN 151 AND 180 THEN 6
      WHEN discharge_age_to_discrep BETWEEN 181 AND 210 THEN 7
      WHEN discharge_age_to_discrep BETWEEN 211 AND 240 THEN 8
      WHEN discharge_age_to_discrep BETWEEN 241 AND 270 THEN 9
      WHEN discharge_age_to_discrep BETWEEN 271 AND 300 THEN 10
      WHEN discharge_age_to_discrep BETWEEN 301 AND 330 THEN 11
      WHEN discharge_age_to_discrep BETWEEN 331 AND 360 THEN 12
      WHEN discharge_age_to_discrep BETWEEN 361 AND 720 THEN 13
      WHEN discharge_age_to_discrep > 720 THEN 14
      ELSE 15
    END AS discharge_age_to_discrep_num,
    CASE
      WHEN discharge_age_to_discrep <= 30 THEN '<= 30'
      WHEN discharge_age_to_discrep BETWEEN 31 AND 60 THEN '31-60'
      WHEN discharge_age_to_discrep BETWEEN 61 AND 90 THEN '61-90'
      WHEN discharge_age_to_discrep BETWEEN 91 AND 120 THEN '91-120'
      WHEN discharge_age_to_discrep BETWEEN 121 AND 150 THEN '121-150'
      WHEN discharge_age_to_discrep BETWEEN 151 AND 180 THEN '151-180'
      WHEN discharge_age_to_discrep BETWEEN 181 AND 210 THEN '181-210'
      WHEN discharge_age_to_discrep BETWEEN 211 AND 240 THEN '211-240'
      WHEN discharge_age_to_discrep BETWEEN 241 AND 270 THEN '241-270'
      WHEN discharge_age_to_discrep BETWEEN 271 AND 300 THEN '271-300'
      WHEN discharge_age_to_discrep BETWEEN 301 AND 330 THEN '301-330'
      WHEN discharge_age_to_discrep BETWEEN 331 AND 360 THEN '331-360'
      WHEN discharge_age_to_discrep BETWEEN 361 AND 720 THEN '361-720'
      WHEN discharge_age_to_discrep > 720 THEN '>720'
      ELSE 'Null Age'
    END AS discharge_age_to_discrep_strat,
    date_diff(DATE(d.extract_date), DATE(d.discharge_date), DAY) AS discharge_age,
    CASE
      WHEN d.payor_due_amount < 0 THEN CASE
        WHEN crt.account_no IS NOT NULL THEN 'Placed on CRT'
        ELSE 'Not on CRT'
      END
      ELSE 'NA'
    END AS crt_flag,
    CASE
      WHEN upper(rtrim(d.reason_code)) IN(
        'UNPROCESSED INTERIM BILL_{O}_[15]', 'UNPROCESSED LATE CHARGES_{O}_[15]', 'UNPROCESSED PARTIAL CHGS_{O}_[15]', 'UNPROCESSED SPLIT BILL_{O}_[15]'
      )
       AND CASE
        WHEN d.payor_due_amount > 0 THEN d.underpayment_age
        WHEN d.payor_due_amount < 0 THEN d.overpayment_age
        ELSE d.non_fin_disc_age
      END <= 60 THEN 'Partial Charges Processed'
      ELSE 'Other'
    END AS exrpt_partchgprcdflag,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Non-Compliant'
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 181 AND 360 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND upper(upper(d.project_name)) LIKE '%CORP_DISPUTE%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND (upper(upper(d.project_name)) NOT LIKE '%CORP_DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'At Risk'
      ELSE 'Undefined'
    END AS status_kpi,
    CASE
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Not Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'At Risk'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Non-Compliant'
      ELSE 'Undefined'
    END AS activity_kpi,
    CASE
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Unvalidated <= 45 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 45 THEN 'Unvalidated <= 45 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'Unvalidated > 45 Days + <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 46 AND 60 THEN 'Unvalidated > 45 Days + <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Potential Overpayment <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Potential Overpayment <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Potential Overpayment > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Potential Overpayment > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Potential Overpayment > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Potential Overpayment > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Validated w/o CRT <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Validated w/o CRT > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Validated w/o CRT > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Validated Not in Appropriate Balance <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'Validated Not in Appropriate Balance > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Validated Not in Appropriate Balance > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) <= 21 THEN 'Validated w/ CRT <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) BETWEEN 22 AND 30 THEN 'Validated w/ CRT > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.last_reason_change_date), DAY) > 30 THEN 'Validated w/ CRT > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) <= 21 THEN 'Validated in Appropriate Balance <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) BETWEEN 22 AND 30 THEN 'Validated in Appropriate Balance > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.collection_date), DAY) > 30 THEN 'Validated in Appropriate Balance > 30 Days'
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Uncoded <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Uncoded <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Uncoded > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Uncoded > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Unvalidated <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Unvalidated <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Unvalidated > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Unvalidated > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment w/o CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment Not in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment w/ CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Potential Overpayment in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment w/o CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment Not in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment w/ CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Potential Overpayment in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated w/o CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated Not in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated w/o CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated Not in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated with CRT <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 120 THEN 'Validated in Appropriate Balance <= 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated with CRT > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 120 THEN 'Validated in Appropriate Balance > 120 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Non-Financial <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Financial > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 21 THEN 'Uncoded <= 21 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END BETWEEN 22 AND 30 THEN 'Uncoded > 21 Days + <= 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 30 THEN 'Uncoded > 30 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.overpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.overpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Potential Overpayment'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Potential Overpayment - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NULL
       AND d.activity_due_date IS NULL THEN 'Potential Overpayment - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NOT NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated w/ CRT'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND crt.account_no IS NULL
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated w/o CRT'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal < 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.pa_total_account_bal >= 0
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated Not in Appropriate Balance'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount < 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.collection_date IS NOT NULL
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      WHEN upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Financial'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Uncoded > 7 Days + <= 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 8 AND 14 THEN 'Uncoded > 7 Days + <= 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Uncoded > 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 14 THEN 'Uncoded > 14 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Unvalidated <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60 THEN 'Unvalidated <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60 THEN 'Unvalidated > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Validated <= 180 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 181 AND 360 THEN 'Validated > 180 Days + <= 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Validated > 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 360 THEN 'Validated > 360 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 180 THEN 'Validated <= 180 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND upper(upper(d.project_name)) LIKE '%CORP_DISPUTE%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Validated > 180 Days (in Corp Dispute)'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND (upper(upper(d.project_name)) NOT LIKE '%CORP_DISPUTE%'
       OR upper(d.project_name) IS NULL)
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 180 THEN 'Validated > 180 Days (Not in Corp Dispute)'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age <= 60 THEN 'Non-Financial <= 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%'
       AND d.non_fin_disc_age > 60 THEN 'Non-Financial > 60 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND upper(trim(d.status_phase_desc)) LIKE '%NON FINANCIAL%' THEN 'Non-Stratified'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 4 THEN 'Uncoded <= 4 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END BETWEEN 5 AND 7 THEN 'Uncoded > 4 Days + <= 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Uncoded > 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NULL
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 7 THEN 'Uncoded > 7 Days'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END <= 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated <= 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND CASE
        WHEN d.underpayment_age IS NULL THEN d.non_fin_disc_age
        ELSE d.underpayment_age
      END > 60
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Unvalidated > 60'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) NOT LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Unvalidated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) < 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) = 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND date_diff(DATE(d.extract_date), DATE(d.activity_due_date), DAY) > 0 THEN 'Validated'
      WHEN upper(rtrim(finclass_flag)) = 'GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      WHEN upper(rtrim(finclass_flag)) = 'NON-GOV'
       AND d.payor_due_amount > 0
       AND d.reason_code IS NOT NULL
       AND upper(d.reason_code) LIKE '%{V}%'
       AND d.activity_due_date IS NULL THEN 'Validated - No Activity'
      ELSE 'Undefined'
    END AS stratification
  FROM
    {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_discrepancy AS d
    LEFT OUTER JOIN 
    {{ params.param_parallon_pbs_views_dataset_name }}.dim_organization AS org ON upper(rtrim(org.coid)) = upper(rtrim(d.coid))
    LEFT OUTER JOIN (
      SELECT
          crt_open_gov_overpayment.account_no,
          crt_open_gov_overpayment.coid,
          crt_open_gov_overpayment.request_date
        FROM
          {{ params.param_parallon_ra_stage_dataset_name }}.crt_open_gov_overpayment
        QUALIFY row_number() OVER (PARTITION BY upper(trim(crt_open_gov_overpayment.account_no)), upper(trim(crt_open_gov_overpayment.coid)) ORDER BY crt_open_gov_overpayment.request_date DESC) = 1
    ) AS crt ON upper(rtrim(d.account_no)) = upper(rtrim(substr(crt.account_no, 1, 38)))
     AND upper(rtrim(d.coid)) = upper(rtrim(crt.coid))
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN d.payor_due_amount < 0 THEN date_diff(DATE(d.overpayment_date), DATE(d.discharge_date), DAY)
        WHEN d.payor_due_amount > 0 THEN date_diff(DATE(d.underpayment_date), DATE(d.discharge_date), DAY)
      END
    ]) AS discharge_age_to_discrep
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN d.payor_financial_class IN(
          1, 2, 3, 6
        ) THEN 'GOV'
        ELSE 'NON-GOV'
      END
    ]) AS finclass_flag
    CROSS JOIN UNNEST(ARRAY[
      date_diff(current_date('US/Central'), DATE(d.last_reason_change_date), DAY)
    ]) AS reason_code_assign_age
;
