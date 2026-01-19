CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.lu_physician_qualifier
AS SELECT
    lu_physician_qualifier.phys_qual_code,
    lu_physician_qualifier.phys_qual_837_code,
    lu_physician_qualifier.phys_qual_desc,
    lu_physician_qualifier.source_system_code,
    lu_physician_qualifier.dw_last_update_date_time
  FROM
    {{ params.param_clm_core_dataset_name }}.lu_physician_qualifier
;
