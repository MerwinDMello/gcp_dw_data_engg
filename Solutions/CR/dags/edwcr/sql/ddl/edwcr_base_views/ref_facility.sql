CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_facility
   OPTIONS(description='Contains the list of facilities for different event types - Imaging, Biopsy, Treatment, Oncology, etc...')
  AS SELECT
      ref_facility.facility_id,
      ref_facility.facility_name,
      ref_facility.source_system_code,
      ref_facility.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_facility
  ;
