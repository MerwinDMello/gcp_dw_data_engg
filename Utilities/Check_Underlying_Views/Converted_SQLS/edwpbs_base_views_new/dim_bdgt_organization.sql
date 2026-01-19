-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_bdgt_organization AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.dim_bdgt_organization
;
