-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_erequest_source_system.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_erequest_source_system
   OPTIONS(description='Reference table to describe Erequest Type')
  AS SELECT
      lu_erequest_source_system.erequest_src_sys_sid,
      lu_erequest_source_system.erequest_src_sys_desc,
      lu_erequest_source_system.erequest_src_sys_category_code,
      lu_erequest_source_system.erequest_src_sys_group_code,
      lu_erequest_source_system.source_system_code,
      lu_erequest_source_system.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.lu_erequest_source_system
  ;
