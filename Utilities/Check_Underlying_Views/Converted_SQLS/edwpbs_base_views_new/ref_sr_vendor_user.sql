-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_sr_vendor_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_sr_vendor_user AS SELECT
    ref_sr_vendor_user.vendor_name,
    ref_sr_vendor_user.user_id,
    ref_sr_vendor_user.first_name,
    ref_sr_vendor_user.last_name,
    ref_sr_vendor_user.user_email_addr,
    ref_sr_vendor_user.job_role_text,
    ref_sr_vendor_user.request_type_text,
    ref_sr_vendor_user.request_status_text,
    ref_sr_vendor_user.location_name,
    ref_sr_vendor_user.workstream_name,
    ref_sr_vendor_user.source_system_code,
    ref_sr_vendor_user.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_sr_vendor_user
;
