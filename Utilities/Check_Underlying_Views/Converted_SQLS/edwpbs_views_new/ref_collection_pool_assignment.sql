-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_collection_pool_assignment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_collection_pool_assignment AS SELECT
    a.pool_assignment_id,
    a.artiva_instance_code,
    a.pool_assignment_desc,
    a.pool_category_desc,
    a.active_ind,
    a.dialer_name,
    a.dialing_status_code,
    a.inclusion_flag,
    a.pool_assigned_location_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_collection_pool_assignment AS a
;
