CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_side
   OPTIONS(description='Contains a distinct list of sides different treatment types could take place.')
  AS SELECT
      ref_side.side_id,
      ref_side.side_desc,
      ref_side.source_system_code,
      ref_side.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_side
  ;
