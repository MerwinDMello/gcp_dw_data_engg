DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_account.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************************************************************************************
 Developer: Sean Wilson
      Name: CC_Account - BTEQ Script.
      Mod1: Creation of script on 8/9/2011. SW.
      Mod2: Changed script for new DDL on 9/6/2011. SW.
      Mod3: Added Diagnostics per Teradata for long running queries on 6/30/2014 FY.
      Mod4: Removed CAST on Patient Account Number on 1/13/2014. AS
      Mod5: Changed JOIN from Fact Facility to Ref_CC_Org_Structure on 2/25/2015 SW.
      Mod6: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
  	  Mod7: CR152 - ICD10 - Add new columns ICD10_DRG_Code, ICD10_DRG_Version_Id, ICD10_DRG_Severity_Level_Code, PA_ICD10_Version_Desc -  09/30/2015  jac
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
      Mod9: Added Audit Merge
  Mod10: 4/29/2022 Added Death Date column as part of Brookdale go live
  Mod11: 8/05/2022 Added Patient_NOA_Date column as part of Brookdale go live
  Mod12: 11/18/2022 Added Archive_State_Ind column as part of PCO 1008 Mon_Account.Ora_archive_state
********************************************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA219;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Diagnostic noprodjoin on for session;
 -- Diagnostic nohashjoin on for session;
 -- Diagnostic noviewfold on for session;
 BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge AS mt USING
  (SELECT DISTINCT a.patient_dw_id,
                   trim(rccos.company_code) AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   ma.id AS account_id,
                   ma.mon_account_linked_to AS linked_pat_account_id,
                   substr(ma.pt_last_name, 1, 100) AS patient_last_name,
                   substr(ma.pt_first_name, 1, 100) AS patient_first_name,
                   substr(ma.pt_middle_name, 1, 30) AS patient_middle_name,
                   substr(ma.pt_name_suffix, 1, 20) AS patient_name_suffix_text,
                   substr(format('%#13.0f', ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ma.pt_social_security_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')), 1, 9) AS social_security_num,
                   substr(ma.medical_record_number, 1, 50) AS medical_record_num,
                   ma.service_date_begin AS admission_date_time,
                   ma.service_date_end AS discharge_date_time,
                   ma.pt_date_of_birth,
                   substr(ma.admit_source, 1, 1) AS admission_source_code,
                   substr(ma.admit_type, 1, 1) AS admission_type_code,
                   substr(ma.financial_class, 1, 3) AS financial_class_code,
                   substr(ma.pt_gender, 1, 1) AS gender_code,
                   substr(ma.hipps_code, 1, 5) AS hipps_code,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ma.patient_status) AS INT64) AS discharge_status_code,
                   substr(ma.billing_status, 1, 1) AS billing_status_code,
                   substr(ma.misc_char03, 1, 3) AS special_bill_ind,
                   substr(ma.misc_char04, 1, 7) AS diag_code_admit,
                   substr(ma.misc_char05, 1, 1) AS cancel_bill_ind,
                   substr(CASE
                              WHEN ma.is_ca_extract_stopped = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS ca_extract_stopped_ind,
                   substr(CASE
                              WHEN ma.is_reassignable = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS reassignable_ind,
                   substr(ma.misc_char01, 1, 4) AS service_code,
                   ma.misc_date01 AS final_bill_date,
                   ma.misc_date02 AS ar_bill_thru_date,
                   CAST(ma.misc_amt03 AS INT64) AS visit_cnt,
                   substr(ma.misc_char08, 1, 3) AS patient_type_code,
                   ma.mon_patient_type_id AS cc_patient_type_id,
                   substr(ma.misc_char02, 1, 3) AS account_status_code,
                   substr(ma.misc_char07, 1, 1) AS marital_status_code,
                   ROUND(ma.misc_amt02, 3, 'ROUND_HALF_EVEN') AS length_of_stay_days_num,
                   substr(ma.drg, 1, 5) AS drg_code,
                   substr(ma.drg_type, 1, 20) AS drg_type,
                   ma.drg_version,
                   substr(ma.drg_regrouped, 1, 5) AS cc_regroup_drg_code,
                   substr(ma.drg_type_regrouped, 1, 20) AS cc_regroup_drg_type,
                   ma.drg_version_regrouped AS cc_regroup_drg_version,
                   substr(CASE
                              WHEN ma.is_archived = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cc_account_archive_ind,
                   CAST(ma.misc_amt04 AS INT64) AS birth_weight_amt,
                   ROUND(ma.misc_amt01, 3, 'ROUND_HALF_EVEN') AS pa_total_account_balance_amt,
                   ROUND(ma.total_charges, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
                   ROUND(ma.total_expected_reimb, 3, 'ROUND_HALF_EVEN') AS total_expected_reimb_amt,
                   ROUND(ma.total_pt_responsibility, 3, 'ROUND_HALF_EVEN') AS total_patient_liability_amt,
                   ROUND(ma.total_payments, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
                   ROUND(ma.total_pt_payments, 3, 'ROUND_HALF_EVEN') AS total_patient_payment_amt,
                   ROUND(ma.total_pt_adjustments, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
                   ROUND(ma.billed_charges, 3, 'ROUND_HALF_EVEN') AS total_billed_charges_amt,
                   ma.mon_accounting_period_id AS financial_period_id,
                   ma.ipf_per_diem_begin_day AS ipf_interupted_day_stay,
                   substr(ma.misc_char10, 1, 1) AS pa_archive_ind,
                   substr(ma.misc_char06, 1, 1) AS pa_purge_ind,
                   substr(usr.login_id, 1, 20) AS updated_login_userid,
                   CAST(ma.date_last_sent_to_calc AS DATETIME) AS last_calc_date_time,
                   ma.misc_date03 AS ins_maint_date,
                   CAST(ma.date_created AS DATETIME) AS create_date_time,
                   CAST(ma.date_updated AS DATETIME) AS update_date_time,
                   substr(ma.icd10_drg, 1, 5) AS icd10_drg_code,
                   ROUND(ma.icd10_drg_vrsn, 0, 'ROUND_HALF_EVEN') AS icd10_drg_version_id,
                   substr(ma.icd10_drg_severity_level, 1, 1) AS icd10_drg_severity_level_code,
                   substr(pvicd.display_text, 1, 15) AS pa_icd_version_desc,
                   ma.death_date AS patient_death_date,
                   ma.noa_date AS patient_noa_date,
                   substr(CASE
                              WHEN upper(trim(ma.ora_archive_state)) = '1' THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS archive_state_ind,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvicd ON pvicd.id = ma.primary_icd_vrsn_id
   AND pvicd.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON ma.schema_id = sm.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON ma.user_id_last_modified = usr.user_id
   AND ma.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_nbr, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_nbr, NUMERIC '1'))
AND (coalesce(mt.account_id, NUMERIC '0') = coalesce(ms.account_id, NUMERIC '0')
     AND coalesce(mt.account_id, NUMERIC '1') = coalesce(ms.account_id, NUMERIC '1'))
AND (coalesce(mt.linked_pat_account_id, NUMERIC '0') = coalesce(ms.linked_pat_account_id, NUMERIC '0')
     AND coalesce(mt.linked_pat_account_id, NUMERIC '1') = coalesce(ms.linked_pat_account_id, NUMERIC '1'))
AND (upper(coalesce(mt.patient_last_name, '0')) = upper(coalesce(ms.patient_last_name, '0'))
     AND upper(coalesce(mt.patient_last_name, '1')) = upper(coalesce(ms.patient_last_name, '1')))
AND (upper(coalesce(mt.patient_first_name, '0')) = upper(coalesce(ms.patient_first_name, '0'))
     AND upper(coalesce(mt.patient_first_name, '1')) = upper(coalesce(ms.patient_first_name, '1')))
AND (upper(coalesce(mt.patient_middle_name, '0')) = upper(coalesce(ms.patient_middle_name, '0'))
     AND upper(coalesce(mt.patient_middle_name, '1')) = upper(coalesce(ms.patient_middle_name, '1')))
AND (upper(coalesce(mt.patient_name_suffix_text, '0')) = upper(coalesce(ms.patient_name_suffix_text, '0'))
     AND upper(coalesce(mt.patient_name_suffix_text, '1')) = upper(coalesce(ms.patient_name_suffix_text, '1')))
AND (upper(coalesce(mt.social_security_num, '0')) = upper(coalesce(ms.social_security_num, '0'))
     AND upper(coalesce(mt.social_security_num, '1')) = upper(coalesce(ms.social_security_num, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (coalesce(mt.admission_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.admission_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.admission_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.admission_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.discharge_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.discharge_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.discharge_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.discharge_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.patient_birth_date, DATE '1970-01-01') = coalesce(ms.pt_date_of_birth, DATE '1970-01-01')
     AND coalesce(mt.patient_birth_date, DATE '1970-01-02') = coalesce(ms.pt_date_of_birth, DATE '1970-01-02'))
AND (upper(coalesce(mt.admission_source_code, '0')) = upper(coalesce(ms.admission_source_code, '0'))
     AND upper(coalesce(mt.admission_source_code, '1')) = upper(coalesce(ms.admission_source_code, '1')))
AND (upper(coalesce(mt.admission_type_code, '0')) = upper(coalesce(ms.admission_type_code, '0'))
     AND upper(coalesce(mt.admission_type_code, '1')) = upper(coalesce(ms.admission_type_code, '1')))
AND (upper(coalesce(mt.financial_class_code, '0')) = upper(coalesce(ms.financial_class_code, '0'))
     AND upper(coalesce(mt.financial_class_code, '1')) = upper(coalesce(ms.financial_class_code, '1')))
AND (upper(coalesce(mt.gender_code, '0')) = upper(coalesce(ms.gender_code, '0'))
     AND upper(coalesce(mt.gender_code, '1')) = upper(coalesce(ms.gender_code, '1')))
AND (upper(coalesce(mt.hipps_code, '0')) = upper(coalesce(ms.hipps_code, '0'))
     AND upper(coalesce(mt.hipps_code, '1')) = upper(coalesce(ms.hipps_code, '1')))
AND (coalesce(mt.discharge_status_code, 0) = coalesce(ms.discharge_status_code, 0)
     AND coalesce(mt.discharge_status_code, 1) = coalesce(ms.discharge_status_code, 1))
AND (upper(coalesce(mt.billing_status_code, '0')) = upper(coalesce(ms.billing_status_code, '0'))
     AND upper(coalesce(mt.billing_status_code, '1')) = upper(coalesce(ms.billing_status_code, '1')))
AND (upper(coalesce(mt.special_bill_ind, '0')) = upper(coalesce(ms.special_bill_ind, '0'))
     AND upper(coalesce(mt.special_bill_ind, '1')) = upper(coalesce(ms.special_bill_ind, '1')))
AND (upper(coalesce(mt.diag_code_admit, '0')) = upper(coalesce(ms.diag_code_admit, '0'))
     AND upper(coalesce(mt.diag_code_admit, '1')) = upper(coalesce(ms.diag_code_admit, '1')))
AND (upper(coalesce(mt.cancel_bill_ind, '0')) = upper(coalesce(ms.cancel_bill_ind, '0'))
     AND upper(coalesce(mt.cancel_bill_ind, '1')) = upper(coalesce(ms.cancel_bill_ind, '1')))
AND (upper(coalesce(mt.ca_extract_stop_ind, '0')) = upper(coalesce(ms.ca_extract_stopped_ind, '0'))
     AND upper(coalesce(mt.ca_extract_stop_ind, '1')) = upper(coalesce(ms.ca_extract_stopped_ind, '1')))
AND (upper(coalesce(mt.reassignable_ind, '0')) = upper(coalesce(ms.reassignable_ind, '0'))
     AND upper(coalesce(mt.reassignable_ind, '1')) = upper(coalesce(ms.reassignable_ind, '1')))
AND (upper(coalesce(mt.service_code, '0')) = upper(coalesce(ms.service_code, '0'))
     AND upper(coalesce(mt.service_code, '1')) = upper(coalesce(ms.service_code, '1')))
AND (coalesce(mt.final_bill_date, DATE '1970-01-01') = coalesce(ms.final_bill_date, DATE '1970-01-01')
     AND coalesce(mt.final_bill_date, DATE '1970-01-02') = coalesce(ms.final_bill_date, DATE '1970-01-02'))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (coalesce(mt.visit_cnt, 0) = coalesce(ms.visit_cnt, 0)
     AND coalesce(mt.visit_cnt, 1) = coalesce(ms.visit_cnt, 1))
AND (upper(coalesce(mt.patient_type_code, '0')) = upper(coalesce(ms.patient_type_code, '0'))
     AND upper(coalesce(mt.patient_type_code, '1')) = upper(coalesce(ms.patient_type_code, '1')))
AND (coalesce(mt.cc_patient_type_id, NUMERIC '0') = coalesce(ms.cc_patient_type_id, NUMERIC '0')
     AND coalesce(mt.cc_patient_type_id, NUMERIC '1') = coalesce(ms.cc_patient_type_id, NUMERIC '1'))
AND (upper(coalesce(mt.account_status_code, '0')) = upper(coalesce(ms.account_status_code, '0'))
     AND upper(coalesce(mt.account_status_code, '1')) = upper(coalesce(ms.account_status_code, '1')))
AND (upper(coalesce(mt.marital_status_code, '0')) = upper(coalesce(ms.marital_status_code, '0'))
     AND upper(coalesce(mt.marital_status_code, '1')) = upper(coalesce(ms.marital_status_code, '1')))
AND (coalesce(mt.length_of_stay_days_num, NUMERIC '0') = coalesce(ms.length_of_stay_days_num, NUMERIC '0')
     AND coalesce(mt.length_of_stay_days_num, NUMERIC '1') = coalesce(ms.length_of_stay_days_num, NUMERIC '1'))
AND (upper(coalesce(mt.drg_code, '0')) = upper(coalesce(ms.drg_code, '0'))
     AND upper(coalesce(mt.drg_code, '1')) = upper(coalesce(ms.drg_code, '1')))
AND (upper(coalesce(mt.drg_type, '0')) = upper(coalesce(ms.drg_type, '0'))
     AND upper(coalesce(mt.drg_type, '1')) = upper(coalesce(ms.drg_type, '1')))
AND (coalesce(mt.drg_version, NUMERIC '0') = coalesce(ms.drg_version, NUMERIC '0')
     AND coalesce(mt.drg_version, NUMERIC '1') = coalesce(ms.drg_version, NUMERIC '1'))
AND (upper(coalesce(mt.cc_regroup_drg_code, '0')) = upper(coalesce(ms.cc_regroup_drg_code, '0'))
     AND upper(coalesce(mt.cc_regroup_drg_code, '1')) = upper(coalesce(ms.cc_regroup_drg_code, '1')))
AND (upper(coalesce(mt.cc_regroup_drg_type, '0')) = upper(coalesce(ms.cc_regroup_drg_type, '0'))
     AND upper(coalesce(mt.cc_regroup_drg_type, '1')) = upper(coalesce(ms.cc_regroup_drg_type, '1')))
AND (coalesce(mt.cc_regroup_drg_version, NUMERIC '0') = coalesce(ms.cc_regroup_drg_version, NUMERIC '0')
     AND coalesce(mt.cc_regroup_drg_version, NUMERIC '1') = coalesce(ms.cc_regroup_drg_version, NUMERIC '1'))
AND (upper(coalesce(mt.cc_account_archive_ind, '0')) = upper(coalesce(ms.cc_account_archive_ind, '0'))
     AND upper(coalesce(mt.cc_account_archive_ind, '1')) = upper(coalesce(ms.cc_account_archive_ind, '1')))
AND (coalesce(mt.birth_weight_amt, 0) = coalesce(ms.birth_weight_amt, 0)
     AND coalesce(mt.birth_weight_amt, 1) = coalesce(ms.birth_weight_amt, 1))
AND (coalesce(mt.pa_total_account_balance_amt, NUMERIC '0') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '0')
     AND coalesce(mt.pa_total_account_balance_amt, NUMERIC '1') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '1'))
AND (coalesce(mt.total_charge_amt, NUMERIC '0') = coalesce(ms.total_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_charge_amt, NUMERIC '1') = coalesce(ms.total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.total_expected_reimb_amt, NUMERIC '0') = coalesce(ms.total_expected_reimb_amt, NUMERIC '0')
     AND coalesce(mt.total_expected_reimb_amt, NUMERIC '1') = coalesce(ms.total_expected_reimb_amt, NUMERIC '1'))
AND (coalesce(mt.total_patient_liability_amt, NUMERIC '0') = coalesce(ms.total_patient_liability_amt, NUMERIC '0')
     AND coalesce(mt.total_patient_liability_amt, NUMERIC '1') = coalesce(ms.total_patient_liability_amt, NUMERIC '1'))
AND (coalesce(mt.total_payment_amt, NUMERIC '0') = coalesce(ms.total_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_payment_amt, NUMERIC '1') = coalesce(ms.total_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_patient_payment_amt, NUMERIC '0') = coalesce(ms.total_patient_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_patient_payment_amt, NUMERIC '1') = coalesce(ms.total_patient_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_adjustment_amt, NUMERIC '0') = coalesce(ms.total_adjustment_amt, NUMERIC '0')
     AND coalesce(mt.total_adjustment_amt, NUMERIC '1') = coalesce(ms.total_adjustment_amt, NUMERIC '1'))
AND (coalesce(mt.total_billed_charges_amt, NUMERIC '0') = coalesce(ms.total_billed_charges_amt, NUMERIC '0')
     AND coalesce(mt.total_billed_charges_amt, NUMERIC '1') = coalesce(ms.total_billed_charges_amt, NUMERIC '1'))
AND (coalesce(mt.financial_period_id, NUMERIC '0') = coalesce(ms.financial_period_id, NUMERIC '0')
     AND coalesce(mt.financial_period_id, NUMERIC '1') = coalesce(ms.financial_period_id, NUMERIC '1'))
AND (coalesce(mt.ipf_interrupted_stay_day, 0) = coalesce(ms.ipf_interupted_day_stay, 0)
     AND coalesce(mt.ipf_interrupted_stay_day, 1) = coalesce(ms.ipf_interupted_day_stay, 1))
AND (upper(coalesce(mt.pa_archive_ind, '0')) = upper(coalesce(ms.pa_archive_ind, '0'))
     AND upper(coalesce(mt.pa_archive_ind, '1')) = upper(coalesce(ms.pa_archive_ind, '1')))
AND (upper(coalesce(mt.pa_purge_ind, '0')) = upper(coalesce(ms.pa_purge_ind, '0'))
     AND upper(coalesce(mt.pa_purge_ind, '1')) = upper(coalesce(ms.pa_purge_ind, '1')))
AND (upper(coalesce(mt.updated_login_userid, '0')) = upper(coalesce(ms.updated_login_userid, '0'))
     AND upper(coalesce(mt.updated_login_userid, '1')) = upper(coalesce(ms.updated_login_userid, '1')))
AND (coalesce(mt.last_calc_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.last_calc_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.last_calc_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.last_calc_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.ins_maint_date, DATE '1970-01-01') = coalesce(ms.ins_maint_date, DATE '1970-01-01')
     AND coalesce(mt.ins_maint_date, DATE '1970-01-02') = coalesce(ms.ins_maint_date, DATE '1970-01-02'))
AND (coalesce(mt.create_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.create_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.create_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.create_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.icd10_drg_code, '0')) = upper(coalesce(ms.icd10_drg_code, '0'))
     AND upper(coalesce(mt.icd10_drg_code, '1')) = upper(coalesce(ms.icd10_drg_code, '1')))
AND (coalesce(mt.icd10_drg_version_id, NUMERIC '0') = coalesce(ms.icd10_drg_version_id, NUMERIC '0')
     AND coalesce(mt.icd10_drg_version_id, NUMERIC '1') = coalesce(ms.icd10_drg_version_id, NUMERIC '1'))
AND (upper(coalesce(mt.icd10_drg_severity_level_code, '0')) = upper(coalesce(ms.icd10_drg_severity_level_code, '0'))
     AND upper(coalesce(mt.icd10_drg_severity_level_code, '1')) = upper(coalesce(ms.icd10_drg_severity_level_code, '1')))
AND (upper(coalesce(mt.pa_icd_version_desc, '0')) = upper(coalesce(ms.pa_icd_version_desc, '0'))
     AND upper(coalesce(mt.pa_icd_version_desc, '1')) = upper(coalesce(ms.pa_icd_version_desc, '1')))
AND (coalesce(mt.patient_death_date, DATE '1970-01-01') = coalesce(ms.patient_death_date, DATE '1970-01-01')
     AND coalesce(mt.patient_death_date, DATE '1970-01-02') = coalesce(ms.patient_death_date, DATE '1970-01-02'))
AND (coalesce(mt.patient_noa_date, DATE '1970-01-01') = coalesce(ms.patient_noa_date, DATE '1970-01-01')
     AND coalesce(mt.patient_noa_date, DATE '1970-01-02') = coalesce(ms.patient_noa_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.archive_state_ind, '0')) = upper(coalesce(ms.archive_state_ind, '0'))
     AND upper(coalesce(mt.archive_state_ind, '1')) = upper(coalesce(ms.archive_state_ind, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        account_id,
        linked_pat_account_id,
        patient_last_name,
        patient_first_name,
        patient_middle_name,
        patient_name_suffix_text,
        social_security_num,
        medical_record_num,
        admission_date_time,
        discharge_date_time,
        patient_birth_date,
        admission_source_code,
        admission_type_code,
        financial_class_code,
        gender_code,
        hipps_code,
        discharge_status_code,
        billing_status_code,
        special_bill_ind,
        diag_code_admit,
        cancel_bill_ind,
        ca_extract_stop_ind,
        reassignable_ind,
        service_code,
        final_bill_date,
        ar_bill_thru_date,
        visit_cnt,
        patient_type_code,
        cc_patient_type_id,
        account_status_code,
        marital_status_code,
        length_of_stay_days_num,
        drg_code,
        drg_type,
        drg_version,
        cc_regroup_drg_code,
        cc_regroup_drg_type,
        cc_regroup_drg_version,
        cc_account_archive_ind,
        birth_weight_amt,
        pa_total_account_balance_amt,
        total_charge_amt,
        total_expected_reimb_amt,
        total_patient_liability_amt,
        total_payment_amt,
        total_patient_payment_amt,
        total_adjustment_amt,
        total_billed_charges_amt,
        financial_period_id,
        ipf_interrupted_stay_day,
        pa_archive_ind,
        pa_purge_ind,
        updated_login_userid,
        last_calc_date_time,
        ins_maint_date,
        create_date_time,
        update_date_time,
        icd10_drg_code,
        icd10_drg_version_id,
        icd10_drg_severity_level_code,
        pa_icd_version_desc,
        patient_death_date,
        patient_noa_date,
        archive_state_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.patient_dw_id, ms.company_cd, ms.coido, ms.unit_num, ms.pat_acct_nbr, ms.account_id, ms.linked_pat_account_id, ms.patient_last_name, ms.patient_first_name, ms.patient_middle_name, ms.patient_name_suffix_text, ms.social_security_num, ms.medical_record_num, ms.admission_date_time, ms.discharge_date_time, ms.pt_date_of_birth, ms.admission_source_code, ms.admission_type_code, ms.financial_class_code, ms.gender_code, ms.hipps_code, ms.discharge_status_code, ms.billing_status_code, ms.special_bill_ind, ms.diag_code_admit, ms.cancel_bill_ind, ms.ca_extract_stopped_ind, ms.reassignable_ind, ms.service_code, ms.final_bill_date, ms.ar_bill_thru_date, ms.visit_cnt, ms.patient_type_code, ms.cc_patient_type_id, ms.account_status_code, ms.marital_status_code, ms.length_of_stay_days_num, ms.drg_code, ms.drg_type, ms.drg_version, ms.cc_regroup_drg_code, ms.cc_regroup_drg_type, ms.cc_regroup_drg_version, ms.cc_account_archive_ind, ms.birth_weight_amt, ms.pa_total_account_balance_amt, ms.total_charge_amt, ms.total_expected_reimb_amt, ms.total_patient_liability_amt, ms.total_payment_amt, ms.total_patient_payment_amt, ms.total_adjustment_amt, ms.total_billed_charges_amt, ms.financial_period_id, ms.ipf_interupted_day_stay, ms.pa_archive_ind, ms.pa_purge_ind, ms.updated_login_userid, ms.last_calc_date_time, ms.ins_maint_date, ms.create_date_time, ms.update_date_time, ms.icd10_drg_code, ms.icd10_drg_version_id, ms.icd10_drg_severity_level_code, ms.pa_icd_version_desc, ms.patient_death_date, ms.patient_noa_date, ms.archive_state_ind, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             account_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge
      GROUP BY patient_dw_id,
               account_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Account_Merge');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_account';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvicd ON pvicd.id = ma.primary_icd_vrsn_id
   AND pvicd.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON ma.schema_id = sm.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON ma.user_id_last_modified = usr.user_id
   AND ma.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
(uuid, table_id, src_sys_nm, src_tbl_nm, tgt_tbl_nm, audit_type, 
expected_value, actual_value, load_start_time, load_end_time, 
load_run_time, job_name, audit_time, audit_status)
VALUES
(GENERATE_UUID(), cast(srctableid as int64), 'ra',srctablename, tgttablename, audit_type,
expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time, job_name, audit_time, audit_status
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account AS x USING
  (SELECT cc_account_merge.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_merge) AS z ON x.patient_dw_id = z.patient_dw_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = z.coid,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    account_id = z.account_id,
    linked_pat_account_id = z.linked_pat_account_id,
    patient_last_name = z.patient_last_name,
    patient_first_name = z.patient_first_name,
    patient_middle_name = z.patient_middle_name,
    patient_name_suffix_text = z.patient_name_suffix_text,
    social_security_num = z.social_security_num,
    medical_record_num = z.medical_record_num,
    admission_date_time = z.admission_date_time,
    discharge_date_time = z.discharge_date_time,
    patient_birth_date = z.patient_birth_date,
    admission_source_code = z.admission_source_code,
    admission_type_code = z.admission_type_code,
    financial_class_code = z.financial_class_code,
    gender_code = z.gender_code,
    hipps_code = z.hipps_code,
    discharge_status_code = z.discharge_status_code,
    billing_status_code = z.billing_status_code,
    special_bill_ind = z.special_bill_ind,
    diag_code_admit = z.diag_code_admit,
    cancel_bill_ind = z.cancel_bill_ind,
    ca_extract_stop_ind = z.ca_extract_stop_ind,
    reassignable_ind = z.reassignable_ind,
    service_code = z.service_code,
    final_bill_date = z.final_bill_date,
    ar_bill_thru_date = z.ar_bill_thru_date,
    visit_cnt = z.visit_cnt,
    patient_type_code = z.patient_type_code,
    cc_patient_type_id = z.cc_patient_type_id,
    account_status_code = z.account_status_code,
    marital_status_code = z.marital_status_code,
    length_of_stay_days_num = z.length_of_stay_days_num,
    drg_code = z.drg_code,
    drg_type = z.drg_type,
    drg_version = z.drg_version,
    cc_regroup_drg_code = z.cc_regroup_drg_code,
    cc_regroup_drg_type = z.cc_regroup_drg_type,
    cc_regroup_drg_version = z.cc_regroup_drg_version,
    cc_account_archive_ind = z.cc_account_archive_ind,
    birth_weight_amt = z.birth_weight_amt,
    pa_total_account_balance_amt = z.pa_total_account_balance_amt,
    total_charge_amt = z.total_charge_amt,
    total_expected_reimb_amt = z.total_expected_reimb_amt,
    total_patient_liability_amt = z.total_patient_liability_amt,
    total_payment_amt = z.total_payment_amt,
    total_patient_payment_amt = z.total_patient_payment_amt,
    total_adjustment_amt = z.total_adjustment_amt,
    total_billed_charges_amt = z.total_billed_charges_amt,
    financial_period_id = z.financial_period_id,
    ipf_interrupted_stay_day = z.ipf_interrupted_stay_day,
    pa_archive_ind = z.pa_archive_ind,
    pa_purge_ind = z.pa_purge_ind,
    updated_login_userid = z.updated_login_userid,
    last_calc_date_time = z.last_calc_date_time,
    ins_maint_date = z.ins_maint_date,
    create_date_time = z.create_date_time,
    update_date_time = z.update_date_time,
    icd10_drg_version_id = z.icd10_drg_version_id,
    icd10_drg_severity_level_code = z.icd10_drg_severity_level_code,
    icd10_drg_code = z.icd10_drg_code,
    pa_icd_version_desc = z.pa_icd_version_desc,
    patient_death_date = z.patient_death_date,
    patient_noa_date = z.patient_noa_date,
    archive_state_ind = z.archive_state_ind,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        account_id,
        linked_pat_account_id,
        patient_last_name,
        patient_first_name,
        patient_middle_name,
        patient_name_suffix_text,
        social_security_num,
        medical_record_num,
        admission_date_time,
        discharge_date_time,
        patient_birth_date,
        admission_source_code,
        admission_type_code,
        financial_class_code,
        gender_code,
        hipps_code,
        discharge_status_code,
        billing_status_code,
        special_bill_ind,
        diag_code_admit,
        cancel_bill_ind,
        ca_extract_stop_ind,
        reassignable_ind,
        service_code,
        final_bill_date,
        ar_bill_thru_date,
        visit_cnt,
        patient_type_code,
        cc_patient_type_id,
        account_status_code,
        marital_status_code,
        length_of_stay_days_num,
        drg_code,
        drg_type,
        drg_version,
        cc_regroup_drg_code,
        cc_regroup_drg_type,
        cc_regroup_drg_version,
        cc_account_archive_ind,
        birth_weight_amt,
        pa_total_account_balance_amt,
        total_charge_amt,
        total_expected_reimb_amt,
        total_patient_liability_amt,
        total_payment_amt,
        total_patient_payment_amt,
        total_adjustment_amt,
        total_billed_charges_amt,
        financial_period_id,
        ipf_interrupted_stay_day,
        pa_archive_ind,
        pa_purge_ind,
        updated_login_userid,
        last_calc_date_time,
        ins_maint_date,
        create_date_time,
        update_date_time,
        icd10_drg_version_id,
        icd10_drg_severity_level_code,
        icd10_drg_code,
        pa_icd_version_desc,
        patient_death_date,
        patient_noa_date,
        archive_state_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.account_id, z.linked_pat_account_id, z.patient_last_name, z.patient_first_name, z.patient_middle_name, z.patient_name_suffix_text, z.social_security_num, z.medical_record_num, z.admission_date_time, z.discharge_date_time, z.patient_birth_date, z.admission_source_code, z.admission_type_code, z.financial_class_code, z.gender_code, z.hipps_code, z.discharge_status_code, z.billing_status_code, z.special_bill_ind, z.diag_code_admit, z.cancel_bill_ind, z.ca_extract_stop_ind, z.reassignable_ind, z.service_code, z.final_bill_date, z.ar_bill_thru_date, z.visit_cnt, z.patient_type_code, z.cc_patient_type_id, z.account_status_code, z.marital_status_code, z.length_of_stay_days_num, z.drg_code, z.drg_type, z.drg_version, z.cc_regroup_drg_code, z.cc_regroup_drg_type, z.cc_regroup_drg_version, z.cc_account_archive_ind, z.birth_weight_amt, z.pa_total_account_balance_amt, z.total_charge_amt, z.total_expected_reimb_amt, z.total_patient_liability_amt, z.total_payment_amt, z.total_patient_payment_amt, z.total_adjustment_amt, z.total_billed_charges_amt, z.financial_period_id, z.ipf_interrupted_stay_day, z.pa_archive_ind, z.pa_purge_ind, z.updated_login_userid, z.last_calc_date_time, z.ins_maint_date, z.create_date_time, z.update_date_time, z.icd10_drg_version_id, z.icd10_drg_severity_level_code, z.icd10_drg_code, z.pa_icd_version_desc, z.patient_death_date, z.patient_noa_date, z.archive_state_ind, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             account_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account
      GROUP BY patient_dw_id,
               account_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Account');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;