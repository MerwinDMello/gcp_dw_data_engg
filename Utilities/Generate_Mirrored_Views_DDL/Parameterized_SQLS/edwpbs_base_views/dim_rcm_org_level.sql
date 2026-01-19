-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_rcm_org_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_rcm_org_level AS SELECT
    dim_rcm_org_level.service_type_name,
    dim_rcm_org_level.fact_lvl_code,
    dim_rcm_org_level.child_fact_lvl_code,
    dim_rcm_org_level.parent_code,
    dim_rcm_org_level.child_code,
    dim_rcm_org_level.parent_desc,
    dim_rcm_org_level.child_desc,
    dim_rcm_org_level.dw_last_update_date_time,
    dim_rcm_org_level.source_system_code
  FROM
    {{ params.param_pbs_core_dataset_name }}.dim_rcm_org_level
;
