-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_vendor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_collection_vendor
   OPTIONS(description='Table contains Vendor\'s information present in the Artiva system')
  AS SELECT
      ref_collection_vendor.vendor_id,
      ref_collection_vendor.vendor_name,
      ref_collection_vendor.artiva_instance_code,
      ref_collection_vendor.source_system_code,
      ref_collection_vendor.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_collection_vendor
  ;
