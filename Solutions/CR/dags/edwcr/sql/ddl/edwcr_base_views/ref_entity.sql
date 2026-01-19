CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_entity AS SELECT
    ref_entity.entity_sid,
    ref_entity.entity_source_system_key,
    ref_entity.indent_level,
    ref_entity.instu_sid,
    ref_entity.company_code,
    ref_entity.coid,
    ref_entity.entity_desc,
    ref_entity.entity_licn_num,
    ref_entity.entity_active_ind,
    ref_entity.entity_type_sid,
    ref_entity.source_system_code,
    ref_entity.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_entity
;
