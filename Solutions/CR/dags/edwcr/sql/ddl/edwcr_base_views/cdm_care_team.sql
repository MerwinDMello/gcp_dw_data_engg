CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_care_team
   OPTIONS(description='Contains information about the providers that were involved in a patient\'s encounter.')
  AS SELECT
      cdm_care_team.patient_dw_id,
      cdm_care_team.role_plyr_sk,
      cdm_care_team.practitioner_role_type,
      cdm_care_team.practitioner_role_type_desc,
      cdm_care_team.company_code,
      cdm_care_team.coid,
      cdm_care_team.practitioner_first_name,
      cdm_care_team.practitioner_middle_name,
      cdm_care_team.practitioner_last_name,
      cdm_care_team.practitioner_full_name,
      cdm_care_team.practitioner_specialty_name,
      cdm_care_team.practitioner_mnemonic_cs,
      cdm_care_team.practitioner_npi,
      cdm_care_team.source_system_code,
      cdm_care_team.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_care_team
  ;
