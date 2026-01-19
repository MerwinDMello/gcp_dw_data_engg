CREATE OR REPLACE VIEW edwclm_bobj_views.lu_physician_type
AS SELECT
    lu_physician_type.phys_type_code,
    lu_physician_type.phys_type_qual_nm101_code,
    lu_physician_type.phys_type_desc,
    lu_physician_type.dw_last_update_date_time,
    lu_physician_type.source_system_code
  FROM
    edwclm_base_views.lu_physician_type
;
