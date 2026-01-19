-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_organization
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Organization')
  AS SELECT
      dim_bdgt_organization.organization_sid,
      dim_bdgt_organization.organization_hier_name,
      dim_bdgt_organization.organization_name_parent,
      dim_bdgt_organization.organization_name_child,
      dim_bdgt_organization.organization_alias_name,
      dim_bdgt_organization.aso_bso_storage_code,
      dim_bdgt_organization.sort_key_num,
      dim_bdgt_organization.consolidation_code,
      dim_bdgt_organization.storage_code,
      dim_bdgt_organization.two_pass_calc_code,
      dim_bdgt_organization.formula_text,
      dim_bdgt_organization.member_solve_order_num,
      dim_bdgt_organization.organization_uda_name,
      dim_bdgt_organization.source_system_code,
      dim_bdgt_organization.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_organization
  ;
