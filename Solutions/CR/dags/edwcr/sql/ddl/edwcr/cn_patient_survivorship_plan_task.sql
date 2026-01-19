CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task
(
  nav_survivorship_plan_task_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each survivorship plan.'),
  task_status_id INT64 OPTIONS(description='A unique identifier for each status.'),
  contact_method_id INT64 OPTIONS(description='A unique identifier for each contact method.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  task_desc_text STRING OPTIONS(description='The description of the task.'),
  task_resolution_date DATE OPTIONS(description='The date the task was resolved.'),
  task_closed_date DATE OPTIONS(description='The date the survivorship task was closed.'),
  contact_result_text STRING OPTIONS(description='A free text field that gives insight into the contact.'),
  contact_date DATE OPTIONS(description='Date the contact was made.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, navigator_id, coid, company_code
OPTIONS(
  description='Contains all the details behind the tasks associated with a patients surviorship plan.'
);
