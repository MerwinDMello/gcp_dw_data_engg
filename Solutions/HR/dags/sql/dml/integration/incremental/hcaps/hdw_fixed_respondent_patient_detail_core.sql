
BEGIN
DECLARE DUP_COUNT INT64;
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail AS st SET coid = stg.coid, dw_last_update_date_time = current_ts FROM (
    SELECT DISTINCT
        fixed_resp_pat_dtl_core_wrk1.respondent_id,
        fixed_resp_pat_dtl_core_wrk1.survey_receive_date,
        fixed_resp_pat_dtl_core_wrk1.respondent_type_code,
        fixed_resp_pat_dtl_core_wrk1.survey_sid,
        fixed_resp_pat_dtl_core_wrk1.company_code,
        fixed_resp_pat_dtl_core_wrk1.coid,
        fixed_resp_pat_dtl_core_wrk1.pat_acct_num,
        fixed_resp_pat_dtl_core_wrk1.patient_dw_id,
        fixed_resp_pat_dtl_core_wrk1.last_name,
        fixed_resp_pat_dtl_core_wrk1.middle_initial_text,
        fixed_resp_pat_dtl_core_wrk1.first_name,
        fixed_resp_pat_dtl_core_wrk1.address_1_text,
        fixed_resp_pat_dtl_core_wrk1.address_2_text,
        fixed_resp_pat_dtl_core_wrk1.city_name,
        fixed_resp_pat_dtl_core_wrk1.state_code,
        fixed_resp_pat_dtl_core_wrk1.zip_code,
        fixed_resp_pat_dtl_core_wrk1.phone_num,
        fixed_resp_pat_dtl_core_wrk1.drg_code,
        fixed_resp_pat_dtl_core_wrk1.gender_code,
        fixed_resp_pat_dtl_core_wrk1.race_code,
        fixed_resp_pat_dtl_core_wrk1.birth_date,
        fixed_resp_pat_dtl_core_wrk1.native_language_code,
        fixed_resp_pat_dtl_core_wrk1.medical_record_num,
        fixed_resp_pat_dtl_core_wrk1.location_code,
        fixed_resp_pat_dtl_core_wrk1.attending_physician_id,
        fixed_resp_pat_dtl_core_wrk1.admission_source_code,
        fixed_resp_pat_dtl_core_wrk1.admission_date,
        fixed_resp_pat_dtl_core_wrk1.admission_time,
        fixed_resp_pat_dtl_core_wrk1.discharge_date,
        fixed_resp_pat_dtl_core_wrk1.discharge_time,
        fixed_resp_pat_dtl_core_wrk1.discharge_status_code,
        fixed_resp_pat_dtl_core_wrk1.unit_code,
        fixed_resp_pat_dtl_core_wrk1.service_code,
        fixed_resp_pat_dtl_core_wrk1.payor_code,
        fixed_resp_pat_dtl_core_wrk1.length_of_stay_days_num,
        fixed_resp_pat_dtl_core_wrk1.room_id_text,
        fixed_resp_pat_dtl_core_wrk1.bed_id_text,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_ind,
        fixed_resp_pat_dtl_core_wrk1.fast_track_code,
        fixed_resp_pat_dtl_core_wrk1.email_text,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_1_id,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_2_id,
        fixed_resp_pat_dtl_core_wrk1.er_admission_ind,
        fixed_resp_pat_dtl_core_wrk1.parent_coid,
        fixed_resp_pat_dtl_core_wrk1.patient_type_code,
        fixed_resp_pat_dtl_core_wrk1.medicare_provider_ccn_num,
        fixed_resp_pat_dtl_core_wrk1.medicare_provider_npi_num,
        fixed_resp_pat_dtl_core_wrk1.coid_name,
        fixed_resp_pat_dtl_core_wrk1.nurse_station_code,
        fixed_resp_pat_dtl_core_wrk1.drg_type_code,
        fixed_resp_pat_dtl_core_wrk1.service_category_code,
        fixed_resp_pat_dtl_core_wrk1.mdc_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_category_code,
        fixed_resp_pat_dtl_core_wrk1.patient_age_num,
        fixed_resp_pat_dtl_core_wrk1.eligible_code,
        fixed_resp_pat_dtl_core_wrk1.exclusion_flag,
        fixed_resp_pat_dtl_core_wrk1.financial_plan_text,
        fixed_resp_pat_dtl_core_wrk1.admitting_physician_num,
        fixed_resp_pat_dtl_core_wrk1.primary_icd_9_code,
        fixed_resp_pat_dtl_core_wrk1.icd_code_version_num,
        fixed_resp_pat_dtl_core_wrk1.cpt_code,
        fixed_resp_pat_dtl_core_wrk1.admission_type_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_1_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_2_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_3_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_4_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_5_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_6_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_7_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_8_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_9_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_10_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_11_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_12_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_13_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_14_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_15_code,
        fixed_resp_pat_dtl_core_wrk1.bill_generated_date,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_1_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_2_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_3_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_4_code,
        fixed_resp_pat_dtl_core_wrk1.surgeon_1_name,
        fixed_resp_pat_dtl_core_wrk1.surgeon_2_name,
        fixed_resp_pat_dtl_core_wrk1.surgeon_3_name,
        fixed_resp_pat_dtl_core_wrk1.attending_physician_name,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_name,
        fixed_resp_pat_dtl_core_wrk1.consulting_physician_id,
        fixed_resp_pat_dtl_core_wrk1.consulting_physician_name,
        fixed_resp_pat_dtl_core_wrk1.ip_ed_flag,
        fixed_resp_pat_dtl_core_wrk1.cpt_ind,
        fixed_resp_pat_dtl_core_wrk1.exclusion_reason_code,
        fixed_resp_pat_dtl_core_wrk1.cms_exclusion_ind,
        fixed_resp_pat_dtl_core_wrk1.ethinicity_code,
        fixed_resp_pat_dtl_core_wrk1.hipps_code,
        fixed_resp_pat_dtl_core_wrk1.arrival_mode_text,
        fixed_resp_pat_dtl_core_wrk1.day_of_week_name,
        fixed_resp_pat_dtl_core_wrk1.cpt_2_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_3_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_4_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_5_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_6_code,
        fixed_resp_pat_dtl_core_wrk1.deceased_ind,
        fixed_resp_pat_dtl_core_wrk1.publicity_ind,
        fixed_resp_pat_dtl_core_wrk1.primary_cpt_service_date,
        fixed_resp_pat_dtl_core_wrk1.final_record_ind,
        fixed_resp_pat_dtl_core_wrk1.source_system_code,
        fixed_resp_pat_dtl_core_wrk1.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1
      WHERE upper(fixed_resp_pat_dtl_core_wrk1.flag) = 'R'
  ) AS stg WHERE st.respondent_id = stg.respondent_id
   AND st.respondent_type_code = stg.respondent_type_code
   AND st.survey_sid = stg.survey_sid
   AND st.survey_receive_date = stg.survey_receive_date;



  UPDATE {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail AS st SET company_code = stg.company_code, coid = stg.coid, pat_acct_num = stg.pat_acct_num, patient_dw_id = stg.patient_dw_id, last_name = stg.last_name, middle_initial_text = stg.middle_initial_text, first_name = stg.first_name, address_1_text = stg.address_1_text, address_2_text = stg.address_2_text, city_name = stg.city_name, state_code = stg.state_code, zip_code = stg.zip_code, phone_num = stg.phone_num, drg_code = stg.drg_code, gender_code = stg.gender_code, race_code = stg.race_code, birth_date = stg.birth_date, native_language_code = stg.native_language_code, medical_record_num = stg.medical_record_num, location_code = stg.location_code, attending_physician_id = stg.attending_physician_id, admission_source_code = stg.admission_source_code, admission_date = stg.admission_date, admission_time = stg.admission_time, discharge_date = stg.discharge_date, discharge_time = stg.discharge_time, discharge_status_code = stg.discharge_status_code, unit_code = stg.unit_code, service_code = stg.service_code, payor_code = stg.payor_code, length_of_stay_days_num = stg.length_of_stay_days_num, room_id_text = stg.room_id_text, bed_id_text = stg.bed_id_text, hospitalist_ind = stg.hospitalist_ind, fast_track_code = stg.fast_track_code, email_text = stg.email_text, hospitalist_1_id = stg.hospitalist_1_id, hospitalist_2_id = stg.hospitalist_2_id, er_admission_ind = stg.er_admission_ind, parent_coid = stg.parent_coid, patient_type_code = stg.patient_type_code, medicare_provider_ccn_num = stg.medicare_provider_ccn_num, medicare_provider_npi_num = stg.medicare_provider_npi_num, coid_name = stg.coid_name, nurse_station_code = stg.nurse_station_code, drg_type_code = stg.drg_type_code, service_category_code = stg.service_category_code, mdc_code = stg.mdc_code, diagnosis_category_code = stg.diagnosis_category_code, patient_age_num = stg.patient_age_num, eligible_code = stg.eligible_code, exclusion_flag = stg.exclusion_flag, financial_plan_text = stg.financial_plan_text, admitting_physician_num = stg.admitting_physician_num, primary_icd_9_code = stg.primary_icd_9_code, icd_code_version_num = stg.icd_code_version_num, cpt_code = stg.cpt_code, admission_type_code = stg.admission_type_code, diagnosis_1_code = stg.diagnosis_1_code, diagnosis_2_code = stg.diagnosis_2_code, diagnosis_3_code = stg.diagnosis_3_code, diagnosis_4_code = stg.diagnosis_4_code, diagnosis_5_code = stg.diagnosis_5_code, diagnosis_6_code = stg.diagnosis_6_code, diagnosis_7_code = stg.diagnosis_7_code, diagnosis_8_code = stg.diagnosis_8_code, diagnosis_9_code = stg.diagnosis_9_code, diagnosis_10_code = stg.diagnosis_10_code, diagnosis_11_code = stg.diagnosis_11_code, diagnosis_12_code = stg.diagnosis_12_code, diagnosis_13_code = stg.diagnosis_13_code, diagnosis_14_code = stg.diagnosis_14_code, diagnosis_15_code = stg.diagnosis_15_code, bill_generated_date = stg.bill_generated_date, surgery_procedure_1_code = stg.surgery_procedure_1_code, surgery_procedure_2_code = stg.surgery_procedure_2_code, surgery_procedure_3_code = stg.surgery_procedure_3_code, surgery_procedure_4_code = stg.surgery_procedure_4_code, surgeon_1_name = stg.surgeon_1_name, surgeon_2_name = stg.surgeon_2_name, surgeon_3_name = stg.surgeon_3_name, attending_physician_name = stg.attending_physician_name, hospitalist_name = stg.hospitalist_name, consulting_physician_id = stg.consulting_physician_id, consulting_physician_name = stg.consulting_physician_name, ip_ed_flag = stg.ip_ed_flag, cpt_ind = stg.cpt_ind, exclusion_reason_code = stg.exclusion_reason_code, cms_exclusion_ind = stg.cms_exclusion_ind, ethinicity_code = stg.ethinicity_code, hipps_code = stg.hipps_code, arrival_mode_text = stg.arrival_mode_text, day_of_week_name = stg.day_of_week_name, cpt_2_code = stg.cpt_2_code, cpt_3_code = stg.cpt_3_code, cpt_4_code = stg.cpt_4_code, cpt_5_code = stg.cpt_5_code, cpt_6_code = stg.cpt_6_code, deceased_ind = stg.deceased_ind, publicity_ind = stg.publicity_ind, primary_cpt_service_date = stg.primary_cpt_service_date, final_record_ind = stg.final_record_ind, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time FROM (
    SELECT DISTINCT
        fixed_resp_pat_dtl_core_wrk1.respondent_id,
        fixed_resp_pat_dtl_core_wrk1.survey_receive_date,
        fixed_resp_pat_dtl_core_wrk1.respondent_type_code,
        fixed_resp_pat_dtl_core_wrk1.survey_sid,
        fixed_resp_pat_dtl_core_wrk1.company_code,
        fixed_resp_pat_dtl_core_wrk1.coid,
        fixed_resp_pat_dtl_core_wrk1.pat_acct_num,
        fixed_resp_pat_dtl_core_wrk1.patient_dw_id,
        fixed_resp_pat_dtl_core_wrk1.last_name,
        fixed_resp_pat_dtl_core_wrk1.middle_initial_text,
        fixed_resp_pat_dtl_core_wrk1.first_name,
        fixed_resp_pat_dtl_core_wrk1.address_1_text,
        fixed_resp_pat_dtl_core_wrk1.address_2_text,
        fixed_resp_pat_dtl_core_wrk1.city_name,
        fixed_resp_pat_dtl_core_wrk1.state_code,
        fixed_resp_pat_dtl_core_wrk1.zip_code,
        fixed_resp_pat_dtl_core_wrk1.phone_num,
        fixed_resp_pat_dtl_core_wrk1.drg_code,
        fixed_resp_pat_dtl_core_wrk1.gender_code,
        fixed_resp_pat_dtl_core_wrk1.race_code,
        fixed_resp_pat_dtl_core_wrk1.birth_date,
        fixed_resp_pat_dtl_core_wrk1.native_language_code,
        fixed_resp_pat_dtl_core_wrk1.medical_record_num,
        fixed_resp_pat_dtl_core_wrk1.location_code,
        fixed_resp_pat_dtl_core_wrk1.attending_physician_id,
        fixed_resp_pat_dtl_core_wrk1.admission_source_code,
        fixed_resp_pat_dtl_core_wrk1.admission_date,
        fixed_resp_pat_dtl_core_wrk1.admission_time,
        fixed_resp_pat_dtl_core_wrk1.discharge_date,
        fixed_resp_pat_dtl_core_wrk1.discharge_time,
        fixed_resp_pat_dtl_core_wrk1.discharge_status_code,
        fixed_resp_pat_dtl_core_wrk1.unit_code,
        fixed_resp_pat_dtl_core_wrk1.service_code,
        fixed_resp_pat_dtl_core_wrk1.payor_code,
        fixed_resp_pat_dtl_core_wrk1.length_of_stay_days_num,
        fixed_resp_pat_dtl_core_wrk1.room_id_text,
        fixed_resp_pat_dtl_core_wrk1.bed_id_text,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_ind,
        fixed_resp_pat_dtl_core_wrk1.fast_track_code,
        fixed_resp_pat_dtl_core_wrk1.email_text,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_1_id,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_2_id,
        fixed_resp_pat_dtl_core_wrk1.er_admission_ind,
        fixed_resp_pat_dtl_core_wrk1.parent_coid,
        fixed_resp_pat_dtl_core_wrk1.patient_type_code,
        fixed_resp_pat_dtl_core_wrk1.medicare_provider_ccn_num,
        fixed_resp_pat_dtl_core_wrk1.medicare_provider_npi_num,
        fixed_resp_pat_dtl_core_wrk1.coid_name,
        fixed_resp_pat_dtl_core_wrk1.nurse_station_code,
        fixed_resp_pat_dtl_core_wrk1.drg_type_code,
        fixed_resp_pat_dtl_core_wrk1.service_category_code,
        fixed_resp_pat_dtl_core_wrk1.mdc_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_category_code,
        fixed_resp_pat_dtl_core_wrk1.patient_age_num,
        fixed_resp_pat_dtl_core_wrk1.eligible_code,
        fixed_resp_pat_dtl_core_wrk1.exclusion_flag,
        fixed_resp_pat_dtl_core_wrk1.financial_plan_text,
        fixed_resp_pat_dtl_core_wrk1.admitting_physician_num,
        fixed_resp_pat_dtl_core_wrk1.primary_icd_9_code,
        fixed_resp_pat_dtl_core_wrk1.icd_code_version_num,
        fixed_resp_pat_dtl_core_wrk1.cpt_code,
        fixed_resp_pat_dtl_core_wrk1.admission_type_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_1_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_2_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_3_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_4_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_5_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_6_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_7_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_8_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_9_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_10_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_11_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_12_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_13_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_14_code,
        fixed_resp_pat_dtl_core_wrk1.diagnosis_15_code,
        fixed_resp_pat_dtl_core_wrk1.bill_generated_date,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_1_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_2_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_3_code,
        fixed_resp_pat_dtl_core_wrk1.surgery_procedure_4_code,
        fixed_resp_pat_dtl_core_wrk1.surgeon_1_name,
        fixed_resp_pat_dtl_core_wrk1.surgeon_2_name,
        fixed_resp_pat_dtl_core_wrk1.surgeon_3_name,
        fixed_resp_pat_dtl_core_wrk1.attending_physician_name,
        fixed_resp_pat_dtl_core_wrk1.hospitalist_name,
        fixed_resp_pat_dtl_core_wrk1.consulting_physician_id,
        fixed_resp_pat_dtl_core_wrk1.consulting_physician_name,
        fixed_resp_pat_dtl_core_wrk1.ip_ed_flag,
        fixed_resp_pat_dtl_core_wrk1.cpt_ind,
        fixed_resp_pat_dtl_core_wrk1.exclusion_reason_code,
        fixed_resp_pat_dtl_core_wrk1.cms_exclusion_ind,
        fixed_resp_pat_dtl_core_wrk1.ethinicity_code,
        fixed_resp_pat_dtl_core_wrk1.hipps_code,
        fixed_resp_pat_dtl_core_wrk1.arrival_mode_text,
        fixed_resp_pat_dtl_core_wrk1.day_of_week_name,
        fixed_resp_pat_dtl_core_wrk1.cpt_2_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_3_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_4_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_5_code,
        fixed_resp_pat_dtl_core_wrk1.cpt_6_code,
        fixed_resp_pat_dtl_core_wrk1.deceased_ind,
        fixed_resp_pat_dtl_core_wrk1.publicity_ind,
        fixed_resp_pat_dtl_core_wrk1.primary_cpt_service_date,
        fixed_resp_pat_dtl_core_wrk1.final_record_ind,
        fixed_resp_pat_dtl_core_wrk1.source_system_code,
        fixed_resp_pat_dtl_core_wrk1.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1
      WHERE upper(fixed_resp_pat_dtl_core_wrk1.flag) = 'S'
      QUALIFY row_number() OVER (PARTITION BY fixed_resp_pat_dtl_core_wrk1.respondent_id, fixed_resp_pat_dtl_core_wrk1.survey_receive_date, fixed_resp_pat_dtl_core_wrk1.respondent_type_code, fixed_resp_pat_dtl_core_wrk1.survey_sid ORDER BY fixed_resp_pat_dtl_core_wrk1.respondent_id, fixed_resp_pat_dtl_core_wrk1.survey_receive_date, fixed_resp_pat_dtl_core_wrk1.respondent_type_code, fixed_resp_pat_dtl_core_wrk1.survey_sid DESC) = 1
  ) AS stg WHERE upper(st.final_record_ind) = 'N'
   AND st.respondent_id = stg.respondent_id
   AND st.respondent_type_code = stg.respondent_type_code
   AND st.survey_sid = stg.survey_sid
   AND st.survey_receive_date = stg.survey_receive_date;

  delete from {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_excp where true;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_excp (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, pat_acct_num, patient_dw_id, last_name, middle_initial_text, first_name, address_1_text, address_2_text, city_name, state_code, zip_code, phone_num, drg_code, gender_code, race_code, birth_date, native_language_code, medical_record_num, location_code, attending_physician_id, admission_source_code, admission_date, admission_time, discharge_date, discharge_time, discharge_status_code, unit_code, service_code, payor_code, length_of_stay_days_num, room_id_text, bed_id_text, hospitalist_ind, fast_track_code, email_text, hospitalist_1_id, hospitalist_2_id, er_admission_ind, parent_coid, patient_type_code, medicare_provider_ccn_num, medicare_provider_npi_num, coid_name, nurse_station_code, drg_type_code, service_category_code, mdc_code, diagnosis_category_code, patient_age_num, eligible_code, exclusion_flag, financial_plan_text, admitting_physician_num, primary_icd_9_code, icd_code_version_num, cpt_code, admission_type_code, diagnosis_1_code, diagnosis_2_code, diagnosis_3_code, diagnosis_4_code, diagnosis_5_code, diagnosis_6_code, diagnosis_7_code, diagnosis_8_code, diagnosis_9_code, diagnosis_10_code, diagnosis_11_code, diagnosis_12_code, diagnosis_13_code, diagnosis_14_code, diagnosis_15_code, bill_generated_date, surgery_procedure_1_code, surgery_procedure_2_code, surgery_procedure_3_code, surgery_procedure_4_code, surgeon_1_name, surgeon_2_name, surgeon_3_name, attending_physician_name, hospitalist_name, consulting_physician_id, consulting_physician_name, ip_ed_flag, cpt_ind, exclusion_reason_code, cms_exclusion_ind, ethinicity_code, hipps_code, arrival_mode_text, day_of_week_name, cpt_2_code, cpt_3_code, cpt_4_code, cpt_5_code, cpt_6_code, deceased_ind, publicity_ind, primary_cpt_service_date, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        stg.respondent_id,
        stg.survey_receive_date,
        stg.respondent_type_code,
        stg.survey_sid,
        stg.company_code,
        stg.coid,
        stg.pat_acct_num,
        stg.patient_dw_id,
        stg.last_name,
        stg.middle_initial_text,
        stg.first_name,
        stg.address_1_text,
        stg.address_2_text,
        stg.city_name,
        stg.state_code,
        stg.zip_code,
        stg.phone_num,
        stg.drg_code,
        stg.gender_code,
        stg.race_code,
        stg.birth_date,
        stg.native_language_code,
        stg.medical_record_num,
        stg.location_code,
        stg.attending_physician_id,
        stg.admission_source_code,
        stg.admission_date,
        stg.admission_time,
        stg.discharge_date,
        stg.discharge_time,
        stg.discharge_status_code,
        stg.unit_code,
        stg.service_code,
        stg.payor_code,
        stg.length_of_stay_days_num,
        stg.room_id_text,
        stg.bed_id_text,
        stg.hospitalist_ind,
        stg.fast_track_code,
        stg.email_text,
        stg.hospitalist_1_id,
        stg.hospitalist_2_id,
        stg.er_admission_ind,
        stg.parent_coid,
        stg.patient_type_code,
        stg.medicare_provider_ccn_num,
        stg.medicare_provider_npi_num,
        stg.coid_name,
        stg.nurse_station_code,
        stg.drg_type_code,
        stg.service_category_code,
        stg.mdc_code,
        stg.diagnosis_category_code,
        stg.patient_age_num,
        stg.eligible_code,
        stg.exclusion_flag,
        stg.financial_plan_text,
        stg.admitting_physician_num,
        stg.primary_icd_9_code,
        stg.icd_code_version_num,
        stg.cpt_code,
        stg.admission_type_code,
        stg.diagnosis_1_code,
        stg.diagnosis_2_code,
        stg.diagnosis_3_code,
        stg.diagnosis_4_code,
        stg.diagnosis_5_code,
        stg.diagnosis_6_code,
        stg.diagnosis_7_code,
        stg.diagnosis_8_code,
        stg.diagnosis_9_code,
        stg.diagnosis_10_code,
        stg.diagnosis_11_code,
        stg.diagnosis_12_code,
        stg.diagnosis_13_code,
        stg.diagnosis_14_code,
        stg.diagnosis_15_code,
        stg.bill_generated_date,
        stg.surgery_procedure_1_code,
        stg.surgery_procedure_2_code,
        stg.surgery_procedure_3_code,
        stg.surgery_procedure_4_code,
        stg.surgeon_1_name,
        stg.surgeon_2_name,
        stg.surgeon_3_name,
        stg.attending_physician_name,
        stg.hospitalist_name,
        stg.consulting_physician_id,
        stg.consulting_physician_name,
        stg.ip_ed_flag,
        stg.cpt_ind,
        stg.exclusion_reason_code,
        stg.cms_exclusion_ind,
        stg.ethinicity_code,
        stg.hipps_code,
        stg.arrival_mode_text,
        stg.day_of_week_name,
        stg.cpt_2_code,
        stg.cpt_3_code,
        stg.cpt_4_code,
        stg.cpt_5_code,
        stg.cpt_6_code,
        stg.deceased_ind,
        stg.publicity_ind,
        stg.primary_cpt_service_date,
        stg.final_record_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1 AS stg
      WHERE (stg.respondent_id, stg.survey_receive_date, stg.respondent_type_code, stg.survey_sid) IN(
        SELECT AS STRUCT
            respondent_id,
            survey_receive_date,
            respondent_type_code,
            survey_sid
          FROM
            {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail
          WHERE upper(final_record_ind) = 'Y'
      )
  ;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, pat_acct_num, patient_dw_id, last_name, middle_initial_text, first_name, address_1_text, address_2_text, city_name, state_code, zip_code, phone_num, drg_code, gender_code, race_code, birth_date, native_language_code, medical_record_num, location_code, attending_physician_id, admission_source_code, admission_date, admission_time, discharge_date, discharge_time, discharge_status_code, unit_code, service_code, payor_code, length_of_stay_days_num, room_id_text, bed_id_text, hospitalist_ind, fast_track_code, email_text, hospitalist_1_id, hospitalist_2_id, er_admission_ind, parent_coid, patient_type_code, medicare_provider_ccn_num, medicare_provider_npi_num, coid_name, nurse_station_code, drg_type_code, service_category_code, mdc_code, diagnosis_category_code, patient_age_num, eligible_code, exclusion_flag, financial_plan_text, admitting_physician_num, primary_icd_9_code, icd_code_version_num, cpt_code, admission_type_code, diagnosis_1_code, diagnosis_2_code, diagnosis_3_code, diagnosis_4_code, diagnosis_5_code, diagnosis_6_code, diagnosis_7_code, diagnosis_8_code, diagnosis_9_code, diagnosis_10_code, diagnosis_11_code, diagnosis_12_code, diagnosis_13_code, diagnosis_14_code, diagnosis_15_code, bill_generated_date, surgery_procedure_1_code, surgery_procedure_2_code, surgery_procedure_3_code, surgery_procedure_4_code, surgeon_1_name, surgeon_2_name, surgeon_3_name, attending_physician_name, hospitalist_name, consulting_physician_id, consulting_physician_name, ip_ed_flag, cpt_ind, exclusion_reason_code, cms_exclusion_ind, ethinicity_code, hipps_code, arrival_mode_text, day_of_week_name, cpt_2_code, cpt_3_code, cpt_4_code, cpt_5_code, cpt_6_code, deceased_ind, publicity_ind, primary_cpt_service_date, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        stg.respondent_id,
        stg.survey_receive_date,
        stg.respondent_type_code,
        stg.survey_sid,
        stg.company_code,
        stg.coid,
        stg.pat_acct_num,
        stg.patient_dw_id,
        stg.last_name,
        stg.middle_initial_text,
        stg.first_name,
        stg.address_1_text,
        stg.address_2_text,
        stg.city_name,
        stg.state_code,
        stg.zip_code,
        stg.phone_num,
        stg.drg_code,
        stg.gender_code,
        stg.race_code,
        stg.birth_date,
        stg.native_language_code,
        stg.medical_record_num,
        stg.location_code,
        stg.attending_physician_id,
        stg.admission_source_code,
        stg.admission_date,
        stg.admission_time,
        stg.discharge_date,
        stg.discharge_time,
        stg.discharge_status_code,
        stg.unit_code,
        stg.service_code,
        stg.payor_code,
        stg.length_of_stay_days_num,
        stg.room_id_text,
        stg.bed_id_text,
        stg.hospitalist_ind,
        stg.fast_track_code,
        stg.email_text,
        stg.hospitalist_1_id,
        stg.hospitalist_2_id,
        stg.er_admission_ind,
        stg.parent_coid,
        stg.patient_type_code,
        stg.medicare_provider_ccn_num,
        stg.medicare_provider_npi_num,
        stg.coid_name,
        stg.nurse_station_code,
        stg.drg_type_code,
        stg.service_category_code,
        stg.mdc_code,
        stg.diagnosis_category_code,
        stg.patient_age_num,
        stg.eligible_code,
        stg.exclusion_flag,
        stg.financial_plan_text,
        stg.admitting_physician_num,
        stg.primary_icd_9_code,
        stg.icd_code_version_num,
        stg.cpt_code,
        stg.admission_type_code,
        stg.diagnosis_1_code,
        stg.diagnosis_2_code,
        stg.diagnosis_3_code,
        stg.diagnosis_4_code,
        stg.diagnosis_5_code,
        stg.diagnosis_6_code,
        stg.diagnosis_7_code,
        stg.diagnosis_8_code,
        stg.diagnosis_9_code,
        stg.diagnosis_10_code,
        stg.diagnosis_11_code,
        stg.diagnosis_12_code,
        stg.diagnosis_13_code,
        stg.diagnosis_14_code,
        stg.diagnosis_15_code,
        stg.bill_generated_date,
        stg.surgery_procedure_1_code,
        stg.surgery_procedure_2_code,
        stg.surgery_procedure_3_code,
        stg.surgery_procedure_4_code,
        stg.surgeon_1_name,
        stg.surgeon_2_name,
        stg.surgeon_3_name,
        stg.attending_physician_name,
        stg.hospitalist_name,
        stg.consulting_physician_id,
        stg.consulting_physician_name,
        stg.ip_ed_flag,
        stg.cpt_ind,
        stg.exclusion_reason_code,
        stg.cms_exclusion_ind,
        stg.ethinicity_code,
        stg.hipps_code,
        stg.arrival_mode_text,
        stg.day_of_week_name,
        stg.cpt_2_code,
        stg.cpt_3_code,
        stg.cpt_4_code,
        stg.cpt_5_code,
        stg.cpt_6_code,
        stg.deceased_ind,
        stg.publicity_ind,
        stg.primary_cpt_service_date,
        stg.final_record_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1 AS stg
      WHERE (stg.respondent_id, stg.survey_receive_date, stg.respondent_type_code, stg.survey_sid) NOT IN(
        SELECT AS STRUCT
            respondent_id,
            survey_receive_date,
            respondent_type_code,
            survey_sid
          FROM
            {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail
      )
      QUALIFY row_number() OVER (PARTITION BY stg.respondent_id, stg.survey_receive_date, stg.respondent_type_code, stg.survey_sid ORDER BY stg.respondent_id, stg.survey_receive_date, stg.respondent_type_code, stg.survey_sid DESC) = 1
  ;



  UPDATE {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail AS tgt SET final_record_ind = 'Y', dw_last_update_date_time = current_ts FROM (
    SELECT
        fixed_resp_pat_dtl_core_wrk1.respondent_id
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk1
      GROUP BY 1
  ) AS stg WHERE upper(tgt.final_record_ind) = 'N'
   AND tgt.respondent_id = stg.respondent_id
   AND 'QUARTERLY' = upper((
    SELECT
        load_type
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_load_type
  ));

  delete from  {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_rej where true;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_rej (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, dw_last_update_date_time, reject_reason)
    SELECT DISTINCT
        wrk2.hca_unique,
        wrk2.surv_id,
        wrk2.adj_samp,
        wrk2.survey_type,
        wrk2.pg_unit,
        wrk2.disdate,
        wrk2.recdate,
        wrk2.mde,
        wrk2.question_id,
        wrk2.coid,
        wrk2.resp,
        wrk2.ptype,
        wrk2.qtype,
        wrk2.category,
        current_ts AS dw_last_update_date_time,
        'Missing COID in Gallup File' AS reject_reason
      FROM
        {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2 AS wrk2
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail AS prd ON upper(coalesce(wrk2.hca_unique, 'H')) = upper(coalesce(prd.hca_unique, 'H'))
         AND wrk2.surv_id = prd.survey_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON wrk2.question_id = sq.question_id
         AND upper(sq.source_system_code) = 'H'
        INNER JOIN
        {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON sq.survey_sid = rs.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS g ON prd.coid = g.coid
        LEFT OUTER JOIN 
        {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = g.coid
        
      WHERE upper(survey_group_text) = 'PATIENT_SATISFACTION'
       AND rs.eff_to_date = '9999-12-31'
       AND sq.eff_to_date = '9999-12-31'
       AND g.coid IS NULL
    UNION ALL
    SELECT DISTINCT        
        a.hca_unique,
        a.surv_id,
        a.adj_samp,
        a.survey_type,
        a.pg_unit,
        a.disdate,
        a.recdate,
        a.mde,
        a.question_id,
        a.coid,
        a.resp,
        a.ptype,
        a.qtype,
        a.category,
        current_ts AS dw_last_update_date_time,
        'Missing QUESTIONID' AS reject_reason
      FROM
        (
          SELECT
              fixed_resp_pat_dtl_core_wrk2.hca_unique,
              fixed_resp_pat_dtl_core_wrk2.surv_id,
              fixed_resp_pat_dtl_core_wrk2.adj_samp,
              fixed_resp_pat_dtl_core_wrk2.survey_type,
              fixed_resp_pat_dtl_core_wrk2.pg_unit,
              fixed_resp_pat_dtl_core_wrk2.disdate,
              fixed_resp_pat_dtl_core_wrk2.recdate,
              fixed_resp_pat_dtl_core_wrk2.mde,
              fixed_resp_pat_dtl_core_wrk2.question_id,
              fixed_resp_pat_dtl_core_wrk2.coid,
              fixed_resp_pat_dtl_core_wrk2.resp,
              fixed_resp_pat_dtl_core_wrk2.ptype,
              fixed_resp_pat_dtl_core_wrk2.qtype,
              fixed_resp_pat_dtl_core_wrk2.category
            FROM
              {{ params.param_hr_stage_dataset_name }}.fixed_resp_pat_dtl_core_wrk2
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
        ) AS a
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS que ON que.question_id = a.question_id
      WHERE que.question_id IS NULL
  ;

      SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT Respondent_Id ,Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID
      FROM {{ params.param_hr_core_dataset_name }}.fixed_respondent_patient_detail
      GROUP BY Respondent_Id ,
Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:fixed_respondent_patient_detail ');
    ELSE
      COMMIT TRANSACTION;
  END IF;
END;