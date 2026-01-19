CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_timeline
   OPTIONS(description='Contains details of timeline for the patient')
  AS SELECT
      cn_patient_timeline.cn_patient_timeline_id,
      cn_patient_timeline.nav_patient_id,
      cn_patient_timeline.tumor_type_id,
      cn_patient_timeline.navigator_id,
      cn_patient_timeline.coid,
      cn_patient_timeline.company_code,
      cn_patient_timeline.nav_referred_date,
      cn_patient_timeline.first_treatment_date,
      cn_patient_timeline.first_consult_date,
      cn_patient_timeline.first_imaging_date,
      cn_patient_timeline.first_medical_oncology_date,
      cn_patient_timeline.first_radiation_oncology_date,
      cn_patient_timeline.first_diagnosis_date,
      cn_patient_timeline.first_biopsy_date,
      cn_patient_timeline.first_surgery_consult_date,
      cn_patient_timeline.first_surgery_date,
      cn_patient_timeline.survivorship_care_plan_close_date,
      cn_patient_timeline.survivorship_care_plan_resolve_date,
      cn_patient_timeline.end_treatment_date,
      cn_patient_timeline.death_date,
      cn_patient_timeline.diagnosis_first_treatment_day_num,
      cn_patient_timeline.diagnosis_first_treatment_available_ind,
      cn_patient_timeline.hashbite_ssk,
      cn_patient_timeline.source_system_code,
      cn_patient_timeline.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_timeline
  ;
