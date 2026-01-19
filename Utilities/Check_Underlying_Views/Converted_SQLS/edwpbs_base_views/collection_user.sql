-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_user
   OPTIONS(description='Standard Artiva Collections User file information')
  AS SELECT
      collection_user.user_id,
      collection_user.artiva_instance_code,
      collection_user.user_dept_num,
      collection_user.user_full_name,
      collection_user.global_vendor_name,
      collection_user.source_system_code,
      collection_user.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.collection_user
  ;
