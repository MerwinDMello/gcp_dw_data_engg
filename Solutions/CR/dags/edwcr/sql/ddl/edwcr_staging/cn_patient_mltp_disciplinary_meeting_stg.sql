CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_mltp_disciplinary_meeting_stg (
cn_patient_mltp_disc_meet_sid INT64 NOT NULL
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, meeting_date DATE
, patient_discussed_ind STRING
, treatment_change_ind STRING
, meeting_notes_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
