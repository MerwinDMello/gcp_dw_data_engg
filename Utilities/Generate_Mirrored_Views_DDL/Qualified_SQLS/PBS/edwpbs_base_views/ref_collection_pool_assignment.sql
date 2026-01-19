-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_pool_assignment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_pool_assignment
   OPTIONS(description='Table contains descriptions for all the collection pool identifiers present in the Artiva system')
  AS SELECT
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
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_pool_assignment
  ;
