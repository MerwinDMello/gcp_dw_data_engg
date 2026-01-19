CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_activity_transaction
   OPTIONS(description='Contains information of Radiation Oncology for activity transaction')
  AS SELECT
      rad_onc_activity_transaction.activity_transaction_sk,
      rad_onc_activity_transaction.activity_priority_sk,
      rad_onc_activity_transaction.hospital_sk,
      rad_onc_activity_transaction.patient_sk,
      rad_onc_activity_transaction.activity_sk,
      rad_onc_activity_transaction.appointment_status_id,
      rad_onc_activity_transaction.actual_resource_type_id,
      rad_onc_activity_transaction.cancel_reason_type_id,
      rad_onc_activity_transaction.appointment_resource_status_id,
      rad_onc_activity_transaction.site_sk,
      rad_onc_activity_transaction.source_activity_transaction_id,
      rad_onc_activity_transaction.schedule_end_date_time,
      rad_onc_activity_transaction.appointment_date_time,
      rad_onc_activity_transaction.appointment_schedule_ind,
      rad_onc_activity_transaction.activity_start_date_time,
      rad_onc_activity_transaction.activity_end_date_time,
      rad_onc_activity_transaction.activity_note_text,
      rad_onc_activity_transaction.check_in_ind,
      rad_onc_activity_transaction.patient_arrival_date_time,
      rad_onc_activity_transaction.waitlist_ind,
      rad_onc_activity_transaction.patient_location_text,
      rad_onc_activity_transaction.appointment_instance_ind,
      rad_onc_activity_transaction.appointment_task_date_time,
      rad_onc_activity_transaction.activity_owner_ind,
      rad_onc_activity_transaction.visit_type_open_chart_ind,
      rad_onc_activity_transaction.resource_id,
      rad_onc_activity_transaction.log_id,
      rad_onc_activity_transaction.run_id,
      rad_onc_activity_transaction.source_system_code,
      rad_onc_activity_transaction.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_activity_transaction
  ;
