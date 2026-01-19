CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_activity_transaction
(
  activity_transaction_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for an activity transaction in EDW'),
  activity_priority_sk INT64 OPTIONS(description='Unique surrogate key generated for activity priority in EDW'),
  hospital_sk INT64 OPTIONS(description='Unique surrogate key generated for a radiation oncology hospital data in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  activity_sk INT64 OPTIONS(description='Unique surrogate key generated for an activity in EDW'),
  appointment_status_id INT64 OPTIONS(description='Identifier for appointment status'),
  actual_resource_type_id INT64 OPTIONS(description='Identifier for actual resource type'),
  cancel_reason_type_id INT64 OPTIONS(description='Identifier for cancel reason type'),
  appointment_resource_status_id INT64 OPTIONS(description='Identifier for appointment reason status'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_activity_transaction_id INT64 NOT NULL OPTIONS(description='Unique identifier for an activity transaction in Radiation Oncology'),
  schedule_end_date_time DATETIME OPTIONS(description='End date time of the schedule'),
  appointment_date_time DATETIME OPTIONS(description='Date time for the appointment'),
  appointment_schedule_ind STRING OPTIONS(description='Indicator for appointment schedule'),
  activity_start_date_time DATETIME OPTIONS(description='Start date time for the activity'),
  activity_end_date_time DATETIME OPTIONS(description='End date time for the activity'),
  activity_note_text STRING OPTIONS(description='Text for the activity note'),
  check_in_ind STRING OPTIONS(description='Indicator for patient has checked in for appointment'),
  patient_arrival_date_time DATETIME OPTIONS(description='Date time for patient arrival'),
  waitlist_ind STRING OPTIONS(description='Indicator for patient waitlist'),
  patient_location_text STRING OPTIONS(description='Text for patient location'),
  appointment_instance_ind STRING OPTIONS(description='Indicator for appointment instance available'),
  appointment_task_date_time DATETIME OPTIONS(description='Date time for appointment task'),
  activity_owner_ind STRING OPTIONS(description='Indicator for activity owner'),
  visit_type_open_chart_ind STRING OPTIONS(description='Indicator for visit type open chart'),
  resource_id INT64 OPTIONS(description='Internal Identification for Aria Resource (physician/staff/machine)'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_activity_transaction_id
OPTIONS(
  description='Contains information of Radiation Oncology for activity transaction'
);
