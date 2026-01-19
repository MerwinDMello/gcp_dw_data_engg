-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_esb_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.dim_esb_organization
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Organization for Essbase Analysis')
  AS SELECT
      CASE
        WHEN upper(dim_esb_organization.min_unit_sid_ind) = 'Y' THEN CASE
           substr(dim_esb_organization.org_name_child, 2, 5)
          WHEN '' THEN 0
          ELSE CAST(substr(dim_esb_organization.org_name_child, 2, 5) as INT64)
        END
        ELSE dim_esb_organization.org_sid
      END AS org_sid,
      dim_esb_organization.lob_sid,
      dim_esb_organization.same_store_sid,
      dim_esb_organization.coid,
      dim_esb_organization.company_code,
      dim_esb_organization.aso_bso_storage_code,
      dim_esb_organization.org_name_parent,
      dim_esb_organization.org_name_child,
      dim_esb_organization.org_alias_name,
      dim_esb_organization.org_coid_alias_name,
      dim_esb_organization.alias_table_name,
      dim_esb_organization.sort_key_num,
      dim_esb_organization.consolidation_code,
      dim_esb_organization.storage_code,
      dim_esb_organization.two_pass_calc_code,
      dim_esb_organization.formula_text,
      dim_esb_organization.member_solve_order_num,
      dim_esb_organization.org_level_uda_name,
      dim_esb_organization.org_hier_name,
      dim_esb_organization.source_system_code,
      dim_esb_organization.dw_last_update_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.dim_esb_organization
  ;
