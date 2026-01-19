CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_radiation_oncology
   OPTIONS(description='Contains the details behind the radiation oncology treatment')
  AS SELECT
      cn_patient_radiation_oncology.cn_patient_radiation_oncology_sid,
      cn_patient_radiation_oncology.treatment_site_location_id,
      cn_patient_radiation_oncology.treatment_type_id,
      cn_patient_radiation_oncology.lung_lobe_location_id,
      cn_patient_radiation_oncology.radiation_oncology_facility_id,
      cn_patient_radiation_oncology.nav_patient_id,
      cn_patient_radiation_oncology.core_record_type_id,
      cn_patient_radiation_oncology.med_spcl_physician_id,
      cn_patient_radiation_oncology.tumor_type_id,
      cn_patient_radiation_oncology.diagnosis_result_id,
      cn_patient_radiation_oncology.nav_diagnosis_id,
      cn_patient_radiation_oncology.navigator_id,
      cn_patient_radiation_oncology.coid,
      cn_patient_radiation_oncology.company_code,
      cn_patient_radiation_oncology.core_record_date,
      cn_patient_radiation_oncology.treatment_start_date,
      cn_patient_radiation_oncology.treatment_end_date,
      cn_patient_radiation_oncology.treatment_fractions_num,
      cn_patient_radiation_oncology.elapse_ind,
      cn_patient_radiation_oncology.elapse_start_date,
      cn_patient_radiation_oncology.elapse_end_date,
      cn_patient_radiation_oncology.radiation_oncology_reason_text,
      cn_patient_radiation_oncology.palliative_ind,
      cn_patient_radiation_oncology.treatment_therapy_schedule_code,
      cn_patient_radiation_oncology.comment_text,
      cn_patient_radiation_oncology.hashbite_ssk,
      cn_patient_radiation_oncology.source_system_code,
      cn_patient_radiation_oncology.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_radiation_oncology
  ;
