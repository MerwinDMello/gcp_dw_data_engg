-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_pool_assignment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_collection_pool_assignment AS SELECT
    ref_collection_pool_assignment.pool_assignment_id,
    ref_collection_pool_assignment.artiva_instance_code,
    ref_collection_pool_assignment.pool_assignment_desc,
    ref_collection_pool_assignment.pool_category_desc,
    ref_collection_pool_assignment.active_ind,
    ref_collection_pool_assignment.dialer_name,
    ref_collection_pool_assignment.dialing_status_code,
    ref_collection_pool_assignment.inclusion_flag,
    ref_collection_pool_assignment.pool_assigned_location_name,
    ref_collection_pool_assignment.source_system_code,
    ref_collection_pool_assignment.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_collection_pool_assignment
;
