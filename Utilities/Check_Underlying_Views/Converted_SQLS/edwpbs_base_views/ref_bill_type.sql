-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_bill_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_bill_type
   OPTIONS(description='Reference  table containing type of Hospital Bill and its grouping based on the Claim frequency')
  AS SELECT
      ref_bill_type.bill_type_code,
      ref_bill_type.bill_type_desc,
      ref_bill_type.bill_type_group_desc,
      ref_bill_type.source_system_code,
      ref_bill_type.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_bill_type
  ;
