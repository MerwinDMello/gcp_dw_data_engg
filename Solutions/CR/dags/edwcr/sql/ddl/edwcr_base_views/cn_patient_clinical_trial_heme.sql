CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_clinical_trial_heme
   OPTIONS(description='Contains the details associated with the clinical trial a patient was enrolled in')
  AS 
	SELECT
	  cn_patient_clinical_trial_sid,
	  nav_patient_id,
	  tumor_type_id,
	  diagnosis_result_id,
	  nav_diagnosis_id,
	  navigator_id,
	  coid,
	  company_code,
	  clinical_trial_name,
	  clinical_trial_enrolled_ind,
	  clinical_trial_enrolled_date,
	  clinical_trial_offered_ind,
	  clinical_trial_offered_date,
	  clinical_trial_evaluated_ind,
	  clinical_trial_evaluated_date,
	  clinical_trial_not_offered_text,
	  clinical_trial_not_offered_other_text,
	  clinical_trial_other_name,
	  not_screened_reason_text,
	  not_screened_other_reason_text,
	  hashbite_ssk,
	  source_system_code,
	  dw_last_update_date_time
	FROM
	  {{ params.param_cr_core_dataset_name }}.cn_patient_clinical_trial_heme
  ;