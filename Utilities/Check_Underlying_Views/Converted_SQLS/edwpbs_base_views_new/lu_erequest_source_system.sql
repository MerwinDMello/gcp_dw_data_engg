-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_erequest_source_system.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_erequest_source_system AS SELECT
    lu_erequest_source_system.erequest_src_sys_sid,
    lu_erequest_source_system.erequest_src_sys_desc,
    lu_erequest_source_system.erequest_src_sys_category_code,
    lu_erequest_source_system.erequest_src_sys_group_code,
    lu_erequest_source_system.source_system_code,
    lu_erequest_source_system.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_erequest_source_system
;
