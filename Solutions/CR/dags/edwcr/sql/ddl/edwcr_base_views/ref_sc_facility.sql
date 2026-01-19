CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_sc_facility
   OPTIONS(description='This tables stores all the markets of Sarah Cannon')
  AS SELECT
      ref_sc_facility.coid,
      ref_sc_facility.coid_name,
      ref_sc_facility.company_code,
      ref_sc_facility.source_system_code,
      ref_sc_facility.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_sc_facility
  ;
