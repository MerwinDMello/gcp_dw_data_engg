-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_sr_vendor_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_sr_vendor_user AS SELECT
    a.vendor_name,
    a.user_id,
    a.first_name,
    a.last_name,
    a.user_email_addr,
    a.job_role_text,
    a.request_type_text,
    a.request_status_text,
    a.location_name,
    a.workstream_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_sr_vendor_user AS a
;
