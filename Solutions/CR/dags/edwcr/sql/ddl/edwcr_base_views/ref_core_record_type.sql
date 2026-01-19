CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type
   OPTIONS(description='Contains a distinct list of status of the patient core record.')
  AS SELECT
      ref_core_record_type.core_record_type_id,
      ref_core_record_type.core_record_type_desc,
      ref_core_record_type.source_system_code,
      ref_core_record_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_core_record_type
  ;
