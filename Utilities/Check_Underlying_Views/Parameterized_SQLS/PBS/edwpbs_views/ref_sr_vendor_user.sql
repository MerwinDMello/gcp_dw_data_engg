-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_sr_vendor_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_sr_vendor_user
   OPTIONS(description='Reference table for Vendor Inventory that has all vendor information and the work stream status.')
  AS SELECT
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
      {{ params.param_pbs_base_views_dataset_name }}.ref_sr_vendor_user AS a
  ;
