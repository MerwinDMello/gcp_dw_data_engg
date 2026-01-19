-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_esb_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_esb_organization AS SELECT
    CASE
      WHEN upper(dim_esb_organization.min_unit_sid_ind) = 'Y' THEN CAST(bqutil.fn.cw_td_normalize_number(substr(dim_esb_organization.org_name_child, 2, 5)) as INT64)
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
    `hca-hin-dev-cur-parallon`.edwpbs.dim_esb_organization
;
