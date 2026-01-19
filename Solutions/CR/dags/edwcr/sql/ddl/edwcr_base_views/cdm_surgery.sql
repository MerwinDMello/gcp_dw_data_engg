CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_surgery
   OPTIONS(description='Contains information regarding each patients surgery.')
  AS SELECT
      cdm_surgery.patient_dw_id,
      cdm_surgery.surgical_case_seq,
      cdm_surgery.activity_date,
      cdm_surgery.coid,
      cdm_surgery.company_code,
      cdm_surgery.pat_acct_num,
      cdm_surgery.primary_surgeon_dw_id,
      cdm_surgery.surgeon_npi,
      cdm_surgery.physician_name,
      cdm_surgery.surgeon_start_date_id,
      cdm_surgery.surgeon_start_time,
      cdm_surgery.surgeon_end_date_id,
      cdm_surgery.surgeon_end_time,
      cdm_surgery.procedure_desc,
      cdm_surgery.source_system_code,
      cdm_surgery.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_surgery
  ;
