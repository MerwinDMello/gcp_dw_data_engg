CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_biopsy
   OPTIONS(description='Contains all the patient biopsies. The data comes from the table PRCDR_DTL and the field PRCDR_TXT has been filtered to only capture those records that contain the word Biopsy.')
  AS SELECT
      cdm_biopsy.procedure_sk,
      cdm_biopsy.procedure_text,
      cdm_biopsy.patient_dw_id,
      cdm_biopsy.coid,
      cdm_biopsy.company_code,
      cdm_biopsy.biopsy_ts,
      cdm_biopsy.biopsy_performing_physician_name,
      cdm_biopsy.physician_specialty_name,
      cdm_biopsy.role_plyr_sk,
      cdm_biopsy.physician_npi,
      cdm_biopsy.priority_sequence_num,
      cdm_biopsy.anesthesia_code_sk,
      cdm_biopsy.anesthesia_code_desc,
      cdm_biopsy.encounter_sk,
      cdm_biopsy.source_system_code,
      cdm_biopsy.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_biopsy
  ;
