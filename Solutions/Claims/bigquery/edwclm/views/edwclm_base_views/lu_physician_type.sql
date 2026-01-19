CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.lu_physician_type
AS SELECT
    lu_physician_type.phys_type_code,
    lu_physician_type.phys_type_qual_nm101_code,
    lu_physician_type.phys_type_desc,
    lu_physician_type.dw_last_update_date_time,
    lu_physician_type.source_system_code
  FROM
    {{ params.param_clm_core_dataset_name }}.lu_physician_type
;
