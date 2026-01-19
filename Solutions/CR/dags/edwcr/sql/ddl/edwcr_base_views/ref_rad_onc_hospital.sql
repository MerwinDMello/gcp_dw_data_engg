CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_hospital
   OPTIONS(description='Contains information for radiation oncology hospital')
  AS SELECT
      ref_rad_onc_hospital.hospital_sk,
      ref_rad_onc_hospital.hospital_address_sk,
      ref_rad_onc_hospital.site_sk,
      ref_rad_onc_hospital.source_hospital_id,
      ref_rad_onc_hospital.coid,
      ref_rad_onc_hospital.company_code,
      ref_rad_onc_hospital.hospital_name,
      ref_rad_onc_hospital.hospital_email_address_text,
      ref_rad_onc_hospital.hospital_history_user_name,
      ref_rad_onc_hospital.hospital_history_date_time,
      ref_rad_onc_hospital.log_id,
      ref_rad_onc_hospital.run_id,
      ref_rad_onc_hospital.source_system_code,
      ref_rad_onc_hospital.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_hospital
  ;
