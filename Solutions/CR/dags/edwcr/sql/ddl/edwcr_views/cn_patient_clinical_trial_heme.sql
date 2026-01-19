/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_clinical_trial_heme
   OPTIONS(description='Contains the details associated with the clinical trial a patient was enrolled in')
  AS 
	SELECT
	  a.cn_patient_clinical_trial_sid,
	  a.nav_patient_id,
	  a.tumor_type_id,
	  a.diagnosis_result_id,
	  a.nav_diagnosis_id,
	  a.navigator_id,
	  a.coid,
	  a.company_code,
	  a.clinical_trial_name,
	  a.clinical_trial_enrolled_ind,
	  a.clinical_trial_enrolled_date,
	  a.clinical_trial_offered_ind,
	  a.clinical_trial_offered_date,
	  a.clinical_trial_evaluated_ind,
	  a.clinical_trial_evaluated_date,
	  a.clinical_trial_not_offered_text,
	  a.clinical_trial_not_offered_other_text,
	  a.clinical_trial_other_name,
	  a.not_screened_reason_text,
	  a.not_screened_other_reason_text,
	  a.hashbite_ssk,
	  a.source_system_code,
	  a.dw_last_update_date_time
	FROM
	  {{ params.param_cr_base_views_dataset_name }}.cn_patient_clinical_trial_heme AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
  ;