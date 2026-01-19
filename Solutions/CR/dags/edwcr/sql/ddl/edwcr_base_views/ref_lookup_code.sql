CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
   OPTIONS(description='This table contains code and description of the columns for which lookup is done')
  AS SELECT
      ref_lookup_code.master_lookup_sid,
      ref_lookup_code.lookup_id,
      ref_lookup_code.lookup_code,
      ref_lookup_code.lookup_sub_code,
      ref_lookup_code.lookup_desc,
      ref_lookup_code.source_system_code,
      ref_lookup_code.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_lookup_code
  ;
