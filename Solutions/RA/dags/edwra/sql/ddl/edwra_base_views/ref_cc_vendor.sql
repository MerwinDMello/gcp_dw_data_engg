-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_vendor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_vendor
   OPTIONS(description='Reference table to all vendors related to an appeal sequence record.')
  AS SELECT
      ref_cc_vendor.company_code,
      ref_cc_vendor.coid,
      ref_cc_vendor.vendor_cd,
      ref_cc_vendor.eff_from_date,
      ref_cc_vendor.vendor_name,
      ref_cc_vendor.vendor_desc,
      ref_cc_vendor.eff_to_date,
      ref_cc_vendor.vendor_creation_user_id,
      ref_cc_vendor.vendor_creation_date_time,
      ref_cc_vendor.vendor_modification_user_id,
      ref_cc_vendor.vendor_modification_date_time,
      ref_cc_vendor.source_system_code,
      ref_cc_vendor.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_vendor
  ;
