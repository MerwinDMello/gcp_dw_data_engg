DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_discrepancy.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/******************************************************************************
 Developer: Sean Wilson
      Name: CC_Discrepancy BTEQ Script.
      Mod1: Creation of script on 10/9/2014. SW.
      Mod2: Added trim to truncated columns found in QA on 12/8/2014 SW.
      Mod3: Change to PDM from Total_Pmt_Cnt to Total_Pmt_Amt on 12/17/2014 SW.
      Mod4: Removed CAST on Patient Account number on 1/14/2015 AS.
      Mod5: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	  Mod6: Added new columns Collection_Amt and Collection_Date.  Also, updated Source_System_Code to get value
	        from staging table instead of hard-coded "N" on 8/26/2015 JAC.
      Mod6: Modified SQL to replace existing table with V_EDW_Daily_Discrepancy_Inventory--04/24.
  Saravana PBI 24439 Changes - 12/03/2019
      Mod10:  -  PBI 25628  - 3/23/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
      Mod11:  -  Added Stats
*******************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA265;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_discrepancy AS x USING
  (SELECT a.patient_dw_id,
          pyro.payor_dw_id,
          dis.payer_rank AS iplan_order_num,
          dis.extract_date AS extract_date_time,
          rcos.company_code,
          dis.coid,
          dis.account_no AS pat_acct_num,
          dis.iplan_id,
          dis.patient_name AS pat_name,
          dis.patient_dob AS pat_birth_date,
          dis.insurance_provider_name,
          dis.payer_group_name AS payor_group_name,
          dis.billing_name,
          dis.billing_contact_name,
          dis.billing_status AS billing_status_cd,
          dis.authorization_code AS authorization_cd,
          dis.payer_patient_id AS payor_pat_cd,
          CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(dis.pa_financial_class) AS INT64) AS pa_financial_class_num,
          dis.payor_financial_class AS payor_financial_class_id,
          dis.pa_service_code AS pa_service_cd,
          dis.pa_account_status AS pa_acct_status_desc,
          dis.pa_discharge_status AS pa_discharge_status_cd,
          dis.pa_patient_type AS pa_pat_type_cd,
          dis.cancel_bill_ind,
          dis.admit_source AS admit_source_cd,
          dis.pa_drg AS pa_drg_cd,
          ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(dis.attending_physician_id) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS attending_physician_id,
          dis.attending_physician_name,
          dis.service_date_begin AS service_begin_date,
          dis.discharge_date,
          dis.cc_patient_type AS cc_pat_type_cd,
          dis.rate_schedule_name,
          dis.rate_schedule_eff_begin_date,
          dis.rate_schedule_eff_end_date,
          dis.project_name AS project_name,
          dis.pa_patient_type AS pa_pat_type_desc,
          dis.work_queue_name,
          dis.comparison_method AS comparison_method_desc,
          dis.reason_code,
          dis.status_category_desc,
          dis.status_desc,
          dis.status_phase_desc,
          dis.calc_date,
          dis.overpayment_date,
          dis.overpayment_age AS overpayment_age_num,
          dis.underpayment_date,
          dis.underpayment_age AS underpayment_age_num,
          dis.non_fin_disc_date AS non_fin_dcrp_date,
          dis.non_fin_disc_age AS non_fin_dcrp_age_num,
          dis.variance_creation_date AS var_creation_date,
          dis.variance_creation_age AS var_creation_age_num,
          dis.variance_resolution_date AS var_resolution_date,
          dis.variance_resolution_age AS var_resolution_age_num,
          dis.first_activity_create_date AS first_actv_create_date,
          dis.last_activity_completion_date AS last_actv_completion_date,
          dis.last_activity_completion_age AS last_actv_completion_age_num,
          dis.last_reason_change_date,
          dis.last_status_change_date,
          dis.last_project_change_date,
          dis.last_owner_change_date,
          dis.activity_due_date AS actv_due_date,
          dis.activity_description AS actv_desc,
          dis.activity_subject AS actv_subject_desc,
          dis.activity_owner AS actv_owner_desc,
          dis.total_payments AS total_pmt_amt,
          dis.pa_actual_los AS pa_actual_los_num,
          dis.total_charges AS total_charge_amt,
          dis.total_billed_charges AS total_billed_charge_amt,
          dis.total_expected_payment AS total_expected_payment_amt,
          dis.total_expected_adjustment AS total_expected_adjustment_amt,
          dis.total_pt_responsibility AS total_pat_resp_amt,
          dis.total_variance_adjustment AS total_var_adjustment_amt,
          dis.total_denial_amount AS total_denial_amt,
          dis.payor_due_amount AS payor_due_amt,
          dis.ar_amount AS ar_amt,
          dis.pa_total_account_bal AS pa_total_acct_bal_amt,
          dis.user_completed_activity_date AS user_completed_actv_date,
          dis.user_completed_activity_age AS user_completed_actv_age_num,
          dis.user_completed_activity_subj AS user_completed_actv_subj_desc,
          dis.user_completed_activity_desc AS user_completed_actv_desc,
          trim(dis.user_completed_activity_ownr) AS user_completed_actv_ownr_34_id,
          dis.user_completed_activity_ownr_name AS user_completed_actv_ownr_name,               
          dis.valid_overpymnt_activity_dt AS valid_over_pmt_actv_date,
          dis.valid_overpymnt_activity_age AS valid_over_pmt_actv_age_num,
          dis.valid_overpymnt_activity_subj AS valid_over_pmt_actv_subj_desc,
          dis.valid_overpymnt_activity_desc AS valid_over_pmt_actv_desc,
          trim(dis.valid_overpymnt_activity_ownr) AS valid_over_pmt_actv_ownr_desc,
          dis.valid_underpymnt_activity_date AS valid_under_pmt_actv_date,
          dis.valid_underpymnt_activity_age AS valid_under_pmt_actv_age_num,
          dis.valid_underpymnt_activity_subj AS valid_under_pmt_actv_subj_desc,
          dis.valid_underpymnt_activity_desc AS valid_under_pmt_actv_desc,
          trim(dis.valid_underpymnt_activity_ownr) AS valid_under_pmt_actv_ownr_id,
          dis.model_issue AS model_issue_desc,
          dis.credit_balance_age AS credit_balance_age_desc,
          edi.stratification AS stratification_group_name, -- -We amy have to change it to Null once Alaga's EDWRA view ready
 -- ,'' as Stratification_Group_Name
 '' AS status_kpi_name, 
 dis.validation_grp AS validation_group_name, --  PBI 24439 CASE LOGIC
 CASE 
     WHEN upper(dis.status_desc) LIKE '%UNREVIEWED%' THEN 'Unreviewed' 
     WHEN upper(dis.status_phase_desc) LIKE '%NON FINANCIAL DISCREPANCY%' THEN 'Non Financial Discrepancy' 
     ELSE dis.discrepancy_group 
 END AS discrepancy_group_name, 
 dis.credit AS credit_category_name, 
 dis.payer_category AS payor_category_name, 
 '' AS activity_kpi_name, --  PBI 24439 CONFIRM FRAN
 dis.collection_amt AS collection_amt, 
 dis.collection_date AS collection_date, --  PBI 24439 NEW FIELDS
 dis.crt_placed_activity_date AS crt_placed_activity_date, 
 dis.last_reason_change_date_2 AS last_reason_change_date_2, 
 dis.last_reason_change_date_3 AS last_reason_change_date_3, 
 dis.last_reason_change_date_4 AS last_reason_change_date_4, 
 dis.max_type5_trans_dt AS max_type_5_trans_date, 
 dis.mon_account_payer_id AS monbot_acct_payer_id, 
 dis.reason_code_2 AS reason_code_2, 
 dis.reason_code_3 AS reason_code_3, 
 dis.reason_code_4 AS reason_code_4, 
 dis.remit_drg_code AS remit_drg_code, 
 dis.sec_establishment_id AS sec_establishment_id, 
 dis.ssc_name AS ssc_name, 
 dis.unit_num AS unit_num, 
 CASE 
     WHEN dis.schema_id = 0 THEN 'P' 
     ELSE 'N' 
 END AS source_system_code, 
 dis.calc_base AS calc_base_desc, --  PBI 24439
 dis.schema_id, --  PBI 24439
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time, --  PBI 24439
 dis.last_remit_received_date AS last_remit_received_date 
   FROM --  PBI 24439
 {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_discrepancy AS dis 
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.v_edw_daily_discrepancy_inventory AS edi ON dis.mon_account_payer_id = edi.mon_account_payer_id 
   AND upper(rtrim(dis.account_no)) = upper(rtrim(edi.account_no)) 
   AND upper(rtrim(dis.coid)) = upper(rtrim(edi.coid)) 
   INNER JOIN -- ---Remove this join once Alaga's EDWRA view ready
 {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rcos ON upper(rtrim(rcos.coid)) = upper(rtrim(dis.coid))
   AND rcos.schema_id = dis.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(dis.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(rcos.company_code))
   AND pyro.iplan_id = dis.iplan_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rcos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rcos.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(dis.account_no) AS FLOAT64)
   WHERE dis.schema_id IN(--  PBI 24439 SCHEMA ID FILTER
 1, 3) ) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_order_num = z.iplan_order_num
AND x.extract_date_time = z.extract_date_time WHEN MATCHED THEN
UPDATE
SET ar_amt = z.ar_amt,
    actv_desc = z.actv_desc,
    actv_due_date = DATE(z.actv_due_date),
    activity_kpi_name = substr(z.activity_kpi_name, 1, 50),
    actv_owner_desc = substr(z.actv_owner_desc, 1, 100),
    actv_subject_desc = z.actv_subject_desc,
    admit_source_cd = substr(z.admit_source_cd, 1, 1),
    attending_physician_id = z.attending_physician_id,
    attending_physician_name = z.attending_physician_name,
    authorization_cd = z.authorization_cd,
    billing_contact_name = z.billing_contact_name,
    billing_name = substr(z.billing_name, 1, 50),
    billing_status_cd = z.billing_status_cd,
    calc_base_desc = z.calc_base_desc,
    calc_date = z.calc_date,
    cancel_bill_ind = substr(z.cancel_bill_ind, 1, 1),
    cc_pat_type_cd = substr(z.cc_pat_type_cd, 1, 10),
    coid = z.coid,
    collection_amt = z.collection_amt,
    collection_date = DATE(z.collection_date),
    company_code = z.company_code,
    comparison_method_desc = substr(z.comparison_method_desc, 1, 40),
    credit_balance_age_desc = substr(z.credit_balance_age_desc, 1, 10),
    credit_category_name = substr(z.credit_category_name, 1, 12),
    crt_placed_activity_date = DATE(z.crt_placed_activity_date),
    discharge_date = DATE(z.discharge_date),
    discrepancy_group_name = substr(z.discrepancy_group_name, 1, 50),
    first_actv_create_date = DATE(z.first_actv_create_date),
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    iplan_id = z.iplan_id,
    insurance_provider_name = z.insurance_provider_name,
    last_actv_completion_age_num = z.last_actv_completion_age_num,
    last_actv_completion_date = DATE(z.last_actv_completion_date),
    last_owner_change_date = DATE(z.last_owner_change_date),
    last_project_change_date = DATE(z.last_project_change_date),
    last_reason_change_date = DATE(z.last_reason_change_date),
    last_reason_change_date_2 = DATE(z.last_reason_change_date_2),
    last_reason_change_date_3 = DATE(z.last_reason_change_date_3),
    last_reason_change_date_4 = DATE(z.last_reason_change_date_4),
    last_remit_received_date = DATE(z.last_remit_received_date),
    last_status_change_date = DATE(z.last_status_change_date),
    max_type_5_trans_date = DATE(z.max_type_5_trans_date),
    model_issue_desc = substr(z.model_issue_desc, 1, 30),
    monbot_acct_payer_id = z.monbot_acct_payer_id,
    non_fin_dcrp_age_num = z.non_fin_dcrp_age_num,
    non_fin_dcrp_date = DATE(z.non_fin_dcrp_date),
    overpayment_age_num = z.overpayment_age_num,
    overpayment_date = DATE(z.overpayment_date),
    pa_acct_status_desc = z.pa_acct_status_desc,
    pa_actual_los_num = CAST(z.pa_actual_los_num AS INT64),
    pa_discharge_status_cd = z.pa_discharge_status_cd,
    pa_drg_cd = z.pa_drg_cd,
    pa_financial_class_num = z.pa_financial_class_num,
    pa_pat_type_desc = z.pa_pat_type_desc,
    pa_service_cd = z.pa_service_cd,
    pa_total_acct_bal_amt = z.pa_total_acct_bal_amt,
    pat_birth_date = z.pat_birth_date,
    pat_name = substr(z.pat_name, 1, 50),
    payor_category_name = z.payor_category_name,
    payor_due_amt = z.payor_due_amt,
    payor_financial_class_id = ROUND(z.payor_financial_class_id, 0, 'ROUND_HALF_EVEN'),
    payor_group_name = substr(z.payor_group_name, 1, 255),
    payor_pat_cd = z.payor_pat_cd,
    project_name = z.project_name,
    rate_schedule_name = z.rate_schedule_name,
    rate_schedule_eff_begin_date = z.rate_schedule_eff_begin_date,
    rate_schedule_eff_end_date = z.rate_schedule_eff_end_date,
    reason_code = z.reason_code,
    reason_code_2 = z.reason_code_2,
    reason_code_3 = substr(z.reason_code_3, 1, 50),
    reason_code_4 = z.reason_code_4,
    remit_drg_code = substr(z.remit_drg_code, 1, 5),
    schema_id = CAST(z.schema_id AS INT64),
    sec_establishment_id = z.sec_establishment_id,
    service_begin_date = DATE(z.service_begin_date),
    ssc_name = substr(z.ssc_name, 1, 100),
    status_category_desc = substr(z.status_category_desc, 1, 10),
    status_desc = z.status_desc,
    status_kpi_name = substr(z.status_kpi_name, 1, 50),
    status_phase_desc = z.status_phase_desc,
    stratification_group_name = substr(z.stratification_group_name, 1, 100),
    total_billed_charge_amt = z.total_billed_charge_amt,
    total_charge_amt = z.total_charge_amt,
    total_denial_amt = z.total_denial_amt,
    total_expected_adjustment_amt = z.total_expected_adjustment_amt,
    total_expected_payment_amt = z.total_expected_payment_amt,
    total_pat_resp_amt = z.total_pat_resp_amt,
    total_pmt_amt = z.total_pmt_amt,
    total_var_adjustment_amt = ROUND(z.total_var_adjustment_amt, 3, 'ROUND_HALF_EVEN'),
    underpayment_age_num = z.underpayment_age_num,
    underpayment_date = DATE(z.underpayment_date),
    unit_num = substr(z.unit_num, 1, 5),
    user_completed_actv_age_num = z.user_completed_actv_age_num,
    user_completed_actv_date = DATE(z.user_completed_actv_date),
    user_completed_actv_desc = z.user_completed_actv_desc,
    user_completed_actv_ownr_34_id = substr(z.user_completed_actv_ownr_34_id, 1, 100),
    user_completed_actv_ownr_name = z.user_completed_actv_ownr_name,       
    user_completed_actv_subj_desc = z.user_completed_actv_subj_desc,
    var_creation_age_num = z.var_creation_age_num,
    var_creation_date = DATE(z.var_creation_date),
    var_resolution_age_num = z.var_resolution_age_num,
    var_resolution_date = DATE(z.var_resolution_date),
    valid_over_pmt_actv_age_num = z.valid_over_pmt_actv_age_num,
    valid_over_pmt_actv_date = DATE(z.valid_over_pmt_actv_date),
    valid_over_pmt_actv_desc = z.valid_over_pmt_actv_desc,
    valid_over_pmt_actv_ownr_desc = substr(z.valid_over_pmt_actv_ownr_desc, 1, 100),
    valid_over_pmt_actv_subj_desc = z.valid_over_pmt_actv_subj_desc,
    valid_under_pmt_actv_age_num = z.valid_under_pmt_actv_age_num,
    valid_under_pmt_actv_date = DATE(z.valid_under_pmt_actv_date),
    valid_under_pmt_actv_desc = z.valid_under_pmt_actv_desc,
    valid_under_pmt_actv_ownr_id = substr(z.valid_under_pmt_actv_ownr_id, 1, 100),
    valid_under_pmt_actv_subj_desc = z.valid_under_pmt_actv_subj_desc,
    validation_group_name = substr(z.validation_group_name, 1, 20),
    work_queue_name = z.work_queue_name,
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        iplan_order_num,
        extract_date_time,
        ar_amt,
        actv_desc,
        actv_due_date,
        activity_kpi_name,
        actv_owner_desc,
        actv_subject_desc,
        admit_source_cd,
        attending_physician_id,
        attending_physician_name,
        authorization_cd,
        billing_contact_name,
        billing_name,
        billing_status_cd,
        calc_base_desc,
        calc_date,
        cancel_bill_ind,
        cc_pat_type_cd,
        coid,
        collection_amt,
        collection_date,
        company_code,
        comparison_method_desc,
        credit_balance_age_desc,
        credit_category_name,
        crt_placed_activity_date,
        discharge_date,
        discrepancy_group_name,
        first_actv_create_date,
        pat_acct_num,
        iplan_id,
        insurance_provider_name,
        last_actv_completion_age_num,
        last_actv_completion_date,
        last_owner_change_date,
        last_project_change_date,
        last_reason_change_date,
        last_reason_change_date_2,
        last_reason_change_date_3,
        last_reason_change_date_4,
        last_remit_received_date,
        last_status_change_date,
        max_type_5_trans_date,
        model_issue_desc,
        monbot_acct_payer_id,
        non_fin_dcrp_age_num,
        non_fin_dcrp_date,
        overpayment_age_num,
        overpayment_date,
        pa_acct_status_desc,
        pa_actual_los_num,
        pa_discharge_status_cd,
        pa_drg_cd,
        pa_financial_class_num,
        pa_pat_type_desc,
        pa_service_cd,
        pa_total_acct_bal_amt,
        pat_birth_date,
        pat_name,
        payor_category_name,
        payor_due_amt,
        payor_financial_class_id,
        payor_group_name,
        payor_pat_cd,
        project_name,
        rate_schedule_name,
        rate_schedule_eff_begin_date,
        rate_schedule_eff_end_date,
        reason_code,
        reason_code_2,
        reason_code_3,
        reason_code_4,
        remit_drg_code,
        schema_id,
        sec_establishment_id,
        service_begin_date,
        ssc_name,
        status_category_desc,
        status_desc,
        status_kpi_name,
        status_phase_desc,
        stratification_group_name,
        total_billed_charge_amt,
        total_charge_amt,
        total_denial_amt,
        total_expected_adjustment_amt,
        total_expected_payment_amt,
        total_pat_resp_amt,
        total_pmt_amt,
        total_var_adjustment_amt,
        underpayment_age_num,
        underpayment_date,
        unit_num,
        user_completed_actv_age_num,
        user_completed_actv_date,
        user_completed_actv_desc,
        user_completed_actv_ownr_34_id,
        user_completed_actv_ownr_name,          
        user_completed_actv_subj_desc,
        var_creation_age_num,
        var_creation_date,
        var_resolution_age_num,
        var_resolution_date,
        valid_over_pmt_actv_age_num,
        valid_over_pmt_actv_date,
        valid_over_pmt_actv_desc,
        valid_over_pmt_actv_ownr_desc,
        valid_over_pmt_actv_subj_desc,
        valid_under_pmt_actv_age_num,
        valid_under_pmt_actv_date,
        valid_under_pmt_actv_desc,
        valid_under_pmt_actv_ownr_id,
        valid_under_pmt_actv_subj_desc,
        validation_group_name,
        work_queue_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.patient_dw_id, z.payor_dw_id, z.iplan_order_num, z.extract_date_time, z.ar_amt, z.actv_desc, DATE(z.actv_due_date), substr(z.activity_kpi_name, 1, 50), substr(z.actv_owner_desc, 1, 100), z.actv_subject_desc, substr(z.admit_source_cd, 1, 1), z.attending_physician_id, z.attending_physician_name, z.authorization_cd, z.billing_contact_name, substr(z.billing_name, 1, 50), z.billing_status_cd, z.calc_base_desc, z.calc_date, substr(z.cancel_bill_ind, 1, 1), substr(z.cc_pat_type_cd, 1, 10), z.coid, z.collection_amt, DATE(z.collection_date), z.company_code, substr(z.comparison_method_desc, 1, 40), substr(z.credit_balance_age_desc, 1, 10), substr(z.credit_category_name, 1, 12), DATE(z.crt_placed_activity_date), DATE(z.discharge_date), substr(z.discrepancy_group_name, 1, 50), DATE(z.first_actv_create_date), ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), z.iplan_id, z.insurance_provider_name, z.last_actv_completion_age_num, DATE(z.last_actv_completion_date), DATE(z.last_owner_change_date), DATE(z.last_project_change_date), DATE(z.last_reason_change_date), DATE(z.last_reason_change_date_2), DATE(z.last_reason_change_date_3), DATE(z.last_reason_change_date_4), DATE(z.last_remit_received_date), DATE(z.last_status_change_date), DATE(z.max_type_5_trans_date), substr(z.model_issue_desc, 1, 30), z.monbot_acct_payer_id, z.non_fin_dcrp_age_num, DATE(z.non_fin_dcrp_date), z.overpayment_age_num, DATE(z.overpayment_date), z.pa_acct_status_desc, CAST(z.pa_actual_los_num AS INT64), z.pa_discharge_status_cd, z.pa_drg_cd, z.pa_financial_class_num, z.pa_pat_type_desc, z.pa_service_cd, z.pa_total_acct_bal_amt, z.pat_birth_date, substr(z.pat_name, 1, 50), z.payor_category_name, z.payor_due_amt, ROUND(z.payor_financial_class_id, 0, 'ROUND_HALF_EVEN'), substr(z.payor_group_name, 1, 255), z.payor_pat_cd, z.project_name, z.rate_schedule_name, z.rate_schedule_eff_begin_date, z.rate_schedule_eff_end_date, z.reason_code, z.reason_code_2, substr(z.reason_code_3, 1, 50), z.reason_code_4, substr(z.remit_drg_code, 1, 5), CAST(z.schema_id AS INT64), z.sec_establishment_id, DATE(z.service_begin_date), substr(z.ssc_name, 1, 100), substr(z.status_category_desc, 1, 10), z.status_desc, substr(z.status_kpi_name, 1, 50), z.status_phase_desc, substr(z.stratification_group_name, 1, 100), z.total_billed_charge_amt, z.total_charge_amt, z.total_denial_amt, z.total_expected_adjustment_amt, z.total_expected_payment_amt, z.total_pat_resp_amt, z.total_pmt_amt, ROUND(z.total_var_adjustment_amt, 3, 'ROUND_HALF_EVEN'), z.underpayment_age_num, DATE(z.underpayment_date), substr(z.unit_num, 1, 5), z.user_completed_actv_age_num, DATE(z.user_completed_actv_date), z.user_completed_actv_desc, substr(z.user_completed_actv_ownr_34_id, 1, 100), z.user_completed_actv_ownr_name, z.user_completed_actv_subj_desc, z.var_creation_age_num, DATE(z.var_creation_date), z.var_resolution_age_num, DATE(z.var_resolution_date), z.valid_over_pmt_actv_age_num, DATE(z.valid_over_pmt_actv_date), z.valid_over_pmt_actv_desc, substr(z.valid_over_pmt_actv_ownr_desc, 1, 100), z.valid_over_pmt_actv_subj_desc, z.valid_under_pmt_actv_age_num, DATE(z.valid_under_pmt_actv_date), z.valid_under_pmt_actv_desc, substr(z.valid_under_pmt_actv_ownr_id, 1, 100), z.valid_under_pmt_actv_subj_desc, substr(z.validation_group_name, 1, 20), z.work_queue_name, substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             iplan_order_num,
             extract_date_time
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_discrepancy
      GROUP BY patient_dw_id,
               payor_dw_id,
               iplan_order_num,
               extract_date_time
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_discrepancy');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- Patient_DW_Id			=			Z.	Patient_DW_Id	,
-- Payor_DW_Id			=			Z.	Payor_DW_Id	,
-- Iplan_Order_Num			=			Z.	Iplan_Order_Num	,
-- Extract_Date_Time			=			Z.	Extract_Date_Time	,
-- Patient_DW_Id=				Z. 		Patient_DW_Id,
-- Payor_DW_Id=				Z. 		Payor_DW_Id,
-- Iplan_Order_Num=				Z. 		Iplan_Order_Num,
-- Extract_Date_Time=				Z. 		Extract_Date_Time,
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Discrepancy');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;