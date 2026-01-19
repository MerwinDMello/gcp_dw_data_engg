-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_credit_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_credit_status AS SELECT
    dim_credit_status.credit_status_sid,
    dim_credit_status.aso_bso_storage_code,
    dim_credit_status.credit_status_name_parent,
    dim_credit_status.credit_status_name_child,
    dim_credit_status.credit_status_child_alias_name,
    dim_credit_status.alias_table_name,
    dim_credit_status.sort_key_num,
    dim_credit_status.consolidation_code,
    dim_credit_status.storage_code,
    dim_credit_status.two_pass_calc_code,
    dim_credit_status.formula_text,
    dim_credit_status.member_solve_order_num,
    dim_credit_status.credit_status_hier_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_credit_status
;
