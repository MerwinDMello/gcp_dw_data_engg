/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS SELECT
    respondent_patient_detail.respondent_id,
    respondent_patient_detail.survey_receive_date,
    respondent_patient_detail.respondent_type_code,
    respondent_patient_detail.survey_sid,
    respondent_patient_detail.company_code,
    respondent_patient_detail.coid,
    respondent_patient_detail.parent_coid,
    respondent_patient_detail.pat_acct_num,
    respondent_patient_detail.patient_dw_id,
    respondent_patient_detail.discharge_date,
    respondent_patient_detail.facility_claim_control_num,
    respondent_patient_detail.exclusion_reason_code,
    respondent_patient_detail.cms_exclusion_ind,
    respondent_patient_detail.final_record_ind,
    respondent_patient_detail.source_system_code,
    respondent_patient_detail.dw_last_update_date_time
  FROM
     {{ params.param_hr_core_dataset_name }}.respondent_patient_detail

