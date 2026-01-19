-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_sr_vendor_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_sr_vendor_user
   OPTIONS(description='Reference table for Vendor Inventory that has all vendor information and the work stream status.')
  AS SELECT
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
      `hca-hin-curated-mirroring-td`.edwpbs.ref_sr_vendor_user
  ;
