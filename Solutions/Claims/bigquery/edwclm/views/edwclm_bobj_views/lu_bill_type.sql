CREATE OR REPLACE VIEW edwclm_bobj_views.lu_bill_type
AS SELECT
    lu_bill_type.bill_type_code,
    lu_bill_type.bill_type_desc,
    lu_bill_type.dw_last_update_date_time,
    lu_bill_type.source_system_code
  FROM
    edwclm_base_views.lu_bill_type
;
