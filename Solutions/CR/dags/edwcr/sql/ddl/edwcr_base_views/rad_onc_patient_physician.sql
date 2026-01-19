CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_physician
   OPTIONS(description='Contains information of physician involved in Radiation Oncology for a patient')
  AS SELECT
      rad_onc_patient_physician.patient_physician_sk,
      rad_onc_patient_physician.site_sk,
      rad_onc_patient_physician.source_patient_physician_id,
      rad_onc_patient_physician.patient_sk,
      rad_onc_patient_physician.primary_physician_ind,
      rad_onc_patient_physician.oncologist_ind,
      rad_onc_patient_physician.resource_id,
      rad_onc_patient_physician.log_id,
      rad_onc_patient_physician.run_id,
      rad_onc_patient_physician.source_system_code,
      rad_onc_patient_physician.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient_physician
  ;
