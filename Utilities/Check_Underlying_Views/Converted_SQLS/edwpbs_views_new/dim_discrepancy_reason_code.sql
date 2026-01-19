-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_discrepancy_reason_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_discrepancy_reason_code AS SELECT
    xxx.reason_code_sid,
    'ASO' AS aso_bso_storage_code,
    xxx.reason_code_parent,
    xxx.reason_code_child,
    xxx.reason_code_alias_name,
    xxx.sort_key_num,
    NULL AS two_pass_calc_code,
    NULL AS consolidation_code,
    NULL AS storage_code,
    NULL AS formula_text,
    0 AS member_solve_order_num,
    'Discr_reason_code_hier' AS reason_code_hier_name,
    NULL AS reason_code_uda_name
  FROM
    (
      SELECT
          eis_reason_code_dim.reason_code_sid,
          max(eis_reason_code_dim.reason_code_gen04) AS reason_code_parent,
          max(eis_reason_code_dim.reason_code_member) AS reason_code_child,
          max(eis_reason_code_dim.reason_code_alias) AS reason_code_alias_name,
          eis_reason_code_dim.reason_code_sort AS sort_key_num
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim
        GROUP BY 1, upper(eis_reason_code_dim.reason_code_gen04), upper(eis_reason_code_dim.reason_code_member), upper(eis_reason_code_dim.reason_code_alias), 5
      UNION ALL
      SELECT
          CAST(eis_reason_code_dim.reason_code_gen04_sort as NUMERIC) AS reason_code_sid,
          max(eis_reason_code_dim.reason_code_gen03) AS reason_code_parent,
          max(eis_reason_code_dim.reason_code_gen04) AS reason_code_child,
          max(eis_reason_code_dim.reason_code_gen04_alias) AS reason_code_alias_name,
          200 AS sort_key_num
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim
        WHERE eis_reason_code_dim.reason_code_sid <> 999
        GROUP BY eis_reason_code_dim.reason_code_gen04_sort, upper(eis_reason_code_dim.reason_code_gen03), upper(eis_reason_code_dim.reason_code_gen04), upper(eis_reason_code_dim.reason_code_gen04_alias)
      UNION ALL
      SELECT
          CAST(eis_reason_code_dim.reason_code_gen03_sort as NUMERIC) AS reason_code_sid,
          substr(CAST(eis_reason_code_dim.reason_code_gen02_sort as STRING), 1, 20) AS reason_code_parent,
          max(eis_reason_code_dim.reason_code_gen03) AS reason_code_child,
          max(eis_reason_code_dim.reason_code_gen03_alias) AS reason_code_alias_name,
          100 AS sort_key_num
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim
        WHERE eis_reason_code_dim.reason_code_sid <> 999
        GROUP BY eis_reason_code_dim.reason_code_gen03_sort, eis_reason_code_dim.reason_code_gen02_sort, upper(eis_reason_code_dim.reason_code_gen03), upper(eis_reason_code_dim.reason_code_gen03_alias)
    ) AS xxx
;
