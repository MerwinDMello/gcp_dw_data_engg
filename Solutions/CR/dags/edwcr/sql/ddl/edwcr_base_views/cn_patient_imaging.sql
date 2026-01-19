CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_imaging
   OPTIONS(description='Contains the details behind the imaging of a patient.')
  AS SELECT
      cn_patient_imaging.cn_patient_imaging_sid,
      cn_patient_imaging.imaging_type_id,
      cn_patient_imaging.imaging_mode_id,
      cn_patient_imaging.imaging_area_side_id,
      cn_patient_imaging.imaging_facility_id,
      cn_patient_imaging.disease_status_id,
      cn_patient_imaging.treatment_status_id,
      cn_patient_imaging.core_record_type_id,
      cn_patient_imaging.nav_patient_id,
      cn_patient_imaging.med_spcl_physician_id,
      cn_patient_imaging.tumor_type_id,
      cn_patient_imaging.diagnosis_result_id,
      cn_patient_imaging.nav_diagnosis_id,
      cn_patient_imaging.navigator_id,
      cn_patient_imaging.coid,
      cn_patient_imaging.company_code,
      cn_patient_imaging.imaging_date,
      cn_patient_imaging.imaging_location_text,
      cn_patient_imaging.birad_scale_code,
      cn_patient_imaging.comment_text,
      cn_patient_imaging.other_image_type_text,
      cn_patient_imaging.initial_diagnosis_ind,
      cn_patient_imaging.disease_monitoring_ind,
      cn_patient_imaging.radiology_result_text,
      cn_patient_imaging.hashbite_ssk,
      cn_patient_imaging.source_system_code,
      cn_patient_imaging.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_imaging
  ;
