CREATE OR REPLACE VIEW edwclm_bobj_views.lu_code_type
AS SELECT
    lu_code_type.code_type_id,
    lu_code_type.code_type_desc,
    lu_code_type.dw_last_update_date_time,
    lu_code_type.source_system_code
  FROM
    edwclm_base_views.lu_code_type
;
