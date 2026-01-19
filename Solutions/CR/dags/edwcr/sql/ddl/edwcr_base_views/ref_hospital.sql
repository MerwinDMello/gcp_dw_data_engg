CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_hospital
   OPTIONS(description='This table contains hospital code and name')
  AS SELECT
      ref_hospital.hospital_id,
      ref_hospital.hospital_code,
      ref_hospital.hospital_name,
      ref_hospital.source_system_code,
      ref_hospital.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_hospital
  ;
