CREATE OR REPLACE VIEW edwclm_bobj_views.lu_physician_qualifier
AS SELECT
    lu_physician_qualifier.phys_qual_code,
    lu_physician_qualifier.phys_qual_837_code,
    lu_physician_qualifier.phys_qual_desc,
    lu_physician_qualifier.source_system_code,
    lu_physician_qualifier.dw_last_update_date_time
  FROM
    edwclm_base_views.lu_physician_qualifier
;
