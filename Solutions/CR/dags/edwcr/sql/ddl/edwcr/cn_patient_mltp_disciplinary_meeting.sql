CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_mltp_disciplinary_meeting
(
  cn_patient_mltp_disciplinary_meet_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  meeting_date DATE OPTIONS(description='The date of the meeting.'),
  patient_discussed_ind STRING OPTIONS(description='Indicates if the patient was discussed in the MDM meeting.'),
  treatment_change_ind STRING OPTIONS(description='Indicates if the treatment was changed based on the meeting.'),
  meeting_notes_text STRING OPTIONS(description='The notes taken during the MDM meeting.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, navigator_id, coid
OPTIONS(
  description='Contains the details behind any meetings of physicians, navigators, specialists around a patient.'
);
