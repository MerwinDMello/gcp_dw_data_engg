-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/edwdim_j_pf_dim_organization_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT trim(format('%20d', count(*))) AS source_string
FROM
  (SELECT dim_org_stg_mthy.coid,
          dim_org_stg_mthy.aso_bso_storage_code,
          dim_org_stg_mthy.org_name_parent,
          dim_org_stg_mthy.org_name_child,
          dim_org_stg_mthy.org_alias_name,
          dim_org_stg_mthy.org_coid_alias_name,
          dim_org_stg_mthy.alias_table_name,
          dim_org_stg_mthy.consolidation_code,
          dim_org_stg_mthy.storage_code,
          dim_org_stg_mthy.two_pass_calc_code,
          dim_org_stg_mthy.formula_text,
          dim_org_stg_mthy.member_solve_order_num,
          dim_org_stg_mthy.org_level_uda_name,
          dim_org_stg_mthy.org_hier_name
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.dim_org_stg_mthy
   UNION DISTINCT SELECT dim_org_stg_mthy_pf_t3185_dnd.coid,
                         dim_org_stg_mthy_pf_t3185_dnd.aso_bso_storage_code,
                         dim_org_stg_mthy_pf_t3185_dnd.org_name_parent,
                         dim_org_stg_mthy_pf_t3185_dnd.org_name_child,
                         dim_org_stg_mthy_pf_t3185_dnd.org_alias_name,
                         dim_org_stg_mthy_pf_t3185_dnd.org_coid_alias_name,
                         dim_org_stg_mthy_pf_t3185_dnd.alias_table_name,
                         dim_org_stg_mthy_pf_t3185_dnd.consolidation_code,
                         dim_org_stg_mthy_pf_t3185_dnd.storage_code,
                         dim_org_stg_mthy_pf_t3185_dnd.two_pass_calc_code,
                         dim_org_stg_mthy_pf_t3185_dnd.formula_text,
                         dim_org_stg_mthy_pf_t3185_dnd.member_solve_order_num,
                         dim_org_stg_mthy_pf_t3185_dnd.org_level_uda_name,
                         dim_org_stg_mthy_pf_t3185_dnd.org_hier_name
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.dim_org_stg_mthy_pf_t3185_dnd
   UNION DISTINCT SELECT dim_esb_organization_homehealth_dnd.coid,
                         dim_esb_organization_homehealth_dnd.aso_bso_storage_code,
                         dim_esb_organization_homehealth_dnd.org_name_parent,
                         dim_esb_organization_homehealth_dnd.org_name_child,
                         dim_esb_organization_homehealth_dnd.org_alias_name,
                         dim_esb_organization_homehealth_dnd.org_coid_alias_name,
                         dim_esb_organization_homehealth_dnd.alias_table_name,
                         dim_esb_organization_homehealth_dnd.consolidation_code,
                         dim_esb_organization_homehealth_dnd.storage_code,
                         dim_esb_organization_homehealth_dnd.two_pass_calc_code,
                         dim_esb_organization_homehealth_dnd.formula_text,
                         dim_esb_organization_homehealth_dnd.member_solve_order_num,
                         dim_esb_organization_homehealth_dnd.org_level_uda_name,
                         dim_esb_organization_homehealth_dnd.org_hier_name
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.dim_esb_organization_homehealth_dnd) AS aa 