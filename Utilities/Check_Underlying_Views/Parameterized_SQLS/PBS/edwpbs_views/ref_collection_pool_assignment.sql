-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_collection_pool_assignment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_collection_pool_assignment
   OPTIONS(description='Table contains descriptions for all the collection pool identifiers present in the Artiva system')
  AS SELECT
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
      {{ params.param_pbs_base_views_dataset_name }}.ref_collection_pool_assignment AS a
  ;
