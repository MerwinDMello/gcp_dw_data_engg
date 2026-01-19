CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.empi_encounter_id_xwalk AS SELECT
    empi_encounter_id_xwalk.encounter_id,
    empi_encounter_id_xwalk.source_system_code,
    empi_encounter_id_xwalk.empi_text,
    empi_encounter_id_xwalk.pat_acct_num,
    empi_encounter_id_xwalk.encounter_subject_area_name,
    empi_encounter_id_xwalk.encounter_id_type_name,
    empi_encounter_id_xwalk.coid,
    empi_encounter_id_xwalk.company_code,
    empi_encounter_id_xwalk.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.empi_encounter_id_xwalk
;
