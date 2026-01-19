-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_user AS SELECT
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
