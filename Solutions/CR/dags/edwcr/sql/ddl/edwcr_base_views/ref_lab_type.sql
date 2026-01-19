CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_lab_type
   OPTIONS(description='Contains a distinct list of patient lab types.')
  AS SELECT
      ref_lab_type.lab_type_id,
      ref_lab_type.lab_type_desc,
      ref_lab_type.source_system_code,
      ref_lab_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_lab_type
  ;
