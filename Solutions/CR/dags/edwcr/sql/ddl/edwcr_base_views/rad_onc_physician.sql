CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_physician
   OPTIONS(description='Contains information of physicians involved in Radiation Oncology')
  AS SELECT
      rad_onc_physician.physician_sk,
      rad_onc_physician.site_sk,
      rad_onc_physician.source_physician_id,
      rad_onc_physician.location_sk,
      rad_onc_physician.resource_type_id,
      rad_onc_physician.physician_first_name,
      rad_onc_physician.physician_last_name,
      rad_onc_physician.physician_suffix_name,
      rad_onc_physician.physician_alias_name,
      rad_onc_physician.physician_title_name,
      rad_onc_physician.physician_email_address_text,
      rad_onc_physician.physician_specialty_text,
      rad_onc_physician.physician_id_text,
      rad_onc_physician.resource_active_ind,
      rad_onc_physician.appointment_schedule_ind,
      rad_onc_physician.physician_start_date_time,
      rad_onc_physician.physician_termination_date_time,
      rad_onc_physician.physician_institution_text,
      rad_onc_physician.physician_comment_text,
      rad_onc_physician.resource_id,
      rad_onc_physician.log_id,
      rad_onc_physician.run_id,
      rad_onc_physician.source_system_code,
      rad_onc_physician.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_physician
  ;
