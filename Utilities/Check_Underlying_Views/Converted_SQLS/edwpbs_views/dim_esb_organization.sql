-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_esb_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_esb_organization
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Organization for Essbase Analysis')
  AS SELECT
      a.org_sid,
      a.lob_sid,
      a.same_store_sid,
      a.coid,
      a.company_code,
      a.aso_bso_storage_code,
      a.org_name_parent,
      a.org_name_child,
      a.org_alias_name,
      a.org_coid_alias_name,
      a.alias_table_name,
      a.sort_key_num,
      a.consolidation_code,
      a.storage_code,
      a.two_pass_calc_code,
      a.formula_text,
      a.member_solve_order_num,
      a.org_level_uda_name,
      a.org_hier_name,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_esb_organization AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
