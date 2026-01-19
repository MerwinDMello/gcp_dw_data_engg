CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.lu_bill_type
AS SELECT
    lu_bill_type.bill_type_code,
    lu_bill_type.bill_type_desc,
    lu_bill_type.dw_last_update_date_time,
    lu_bill_type.source_system_code
  FROM
    {{ params.param_clm_core_dataset_name }}.lu_bill_type
;
